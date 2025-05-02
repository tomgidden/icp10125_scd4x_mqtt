#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script reads data from ICP10125 (pressure/temperature) and SCD4X (CO2/temperature/humidity)
sensors and publishes the readings to an MQTT broker.
"""

import json
import logging
import paho.mqtt.client as paho
import icp10125
import scd4x
import time
import os
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

mqtt_host = os.getenv('MQTT_HOST', 'mqtt.home')
topic = os.getenv('MQTT_TOPIC', 'environment')
retain = os.getenv('MQTT_RETAIN', 'False').lower() in ('true', '1', 'yes')
temperature_calibrate = float(os.getenv('TEMPERATURE_CALIBRATE', '0'))
frequency = int(os.getenv('FREQUENCY', '60'))
altitude = int(os.getenv('ALTITUDE', '17'))

if os.getenv('DEBUG', 'False').lower() in ('true', '1', 'yes'):   level = logging.DEBUG
elif os.getenv('QUIET', 'False').lower() in ('true', '1', 'yes'): level = logging.WARNING
else: level = logging.INFO

logging.basicConfig(
    level=level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def on_connect(mqtt, userdata, flags, rc):
    """Callback for when the client connects to the MQTT broker"""
    if rc == 0:
        logger.info(f"Connected to MQTT broker at {mqtt_host}")
    else:
        logger.error(f"Failed to connect to MQTT broker with code: {rc}")

def on_disconnect(mqtt, userdata, rc):
    """Callback for when the client disconnects from the MQTT broker"""
    logger.warning(f"Disconnected from MQTT server with code: {rc}")
    while rc != 0:
        try:
            time.sleep(1)
            rc = mqtt.reconnect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            time.sleep(5)  # Wait longer after a failed reconnection attempt
            pass
    logger.info("Reconnected to MQTT server.")

def on_publish(mqtt, userdata, mid):
    """Callback for when a message is published to the MQTT broker"""
    logger.debug(f"Message {mid} published successfully")

# Initialize MQTT client
logger.info(f"Initializing MQTT client and connecting to {mqtt_host}")
mqtt = paho.Client()
mqtt.on_connect = on_connect
mqtt.on_disconnect = on_disconnect
mqtt.on_publish = on_publish

# Connect to MQTT broker
try:
    mqtt.connect(mqtt_host, 1883, 60)
    mqtt.loop_start()
except Exception as e:
    logger.error(f"Failed to connect to MQTT broker: {e}")
    raise

def publish_sensor_data(mqtt, sensor_data):
    """
    Publish sensor data to individual MQTT topics and as a combined JSON payload
    """
    timestamp = sensor_data['timestamp']

    # Publish individual readings to separate topics
    mqtt.publish(topic + '/watchdog', '1', retain=False)

    for key in ['humidity', 'pressure', 'temperature', 'co2']:
        if key in sensor_data:
            mqtt.publish(topic + '/' + key, sensor_data[key], retain=retain)
            mqtt.publish(topic + '/' + key + '/timestamp', timestamp, retain=retain)
            logger.debug(f"Published {key}: {sensor_data[key]}")

    # Publish all readings as a single JSON payload
    mqtt.publish(topic, json.dumps(sensor_data), retain=retain)
    logger.info(sensor_data)

def initialize_sensors():
    """Initialize the ICP10125 and SCD4X sensors"""
    logger.debug("Initializing sensors...")
    try:



        return icp, scd
    except Exception as e:
        logger.error(f"Error initializing sensors: {e}")
        raise

def main():
    """Main function to run the sensor reading and MQTT publishing loop"""
    logger.info("Starting sensor monitoring")

    try:
        # Initialize sensors
        icp = icp10125.ICP10125()
        logger.info("ICP10125 sensor initialized")

        scd = scd4x.SCD4X(quiet=False)
        scd.set_altitude(int(altitude))
        scd.start_periodic_measurement(low_power=False)
        logger.info("SCD4X sensor initialized and periodic measurement started")

        # scd.set_temperature_offset(...)

        # Main measurement loop
        logger.info(f"Entering main measurement loop with {frequency} second intervals")
        measurement_count = 0

        while True:
            try:
                measurement_count += 1
                start_time = time.time()
                logger.debug(f"Taking measurement #{measurement_count}")

                # Measure pressure and temperature from ICP10125
                pressure, temperature = icp.measure(measure_command=icp10125.ULTRA_LOW_NOISE)
                logger.debug(f"ICP10125 readings - Pressure: {pressure} hPa ({int(pressure/100)}), Temp: {temperature}°C")

#                logger.debug(f"SCD - {scd.get_ambient_pressure()} pressure")
                scd.set_ambient_pressure(int(pressure/100))

#                scd.set_automatic_self_calibration_enabled(True)

                # Measure CO2, temperature, and humidity from SCD4X
                co2, temperatureA, humidity, timestamp = scd.measure(blocking=True, timeout=10)
                logger.debug(f"SCD4X readings - CO2: {co2} ppm, Temp: {temperatureA}°C, Humidity: {humidity}%")

                # Note, as SCD4X contains a heater for the CO2 sensor, temperatureA has a built-in fudge factor
                # which can be calibrated, but the sensor on the isolated ICP10125 should be better anyway.
                # The relative humidity calculated by SCD4X, though, will be questionable.

                #scd.set_temperature_offset(...)

                # Create sensor data dictionary
                timestamp = int(timestamp)
                sensor_data = {
                    'temperature': temperature + temperature_calibrate,
                    'temperatureA': temperatureA,
                    'humidity': humidity,
                    'pressure': pressure,
                    'co2': co2,
                    'timestamp': timestamp
                }

                # Publish data to MQTT
                publish_sensor_data(mqtt, sensor_data)

                # Calculate time spent on measurements and publishing
                elapsed_time = time.time() - start_time
                logger.debug(f"Measurement and publishing took {elapsed_time:.2f} seconds")

                # Sleep for the remaining time to maintain the desired frequency
                sleep_time = max(0, frequency - elapsed_time)
                if sleep_time > 0:
                    logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"Measurement cycle took longer than frequency setting by {-sleep_time:.2f} seconds")

            except IOError as e:
                logger.error(f"IOError during measurement: {e}")
                time.sleep(3)  # Wait before retrying

            except Exception as e:
                logger.error(f"Unexpected error during measurement: {e}")
                time.sleep(5)  # Wait longer for unexpected errors

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down")
    finally:
        # Clean up
        logger.info("Stopping MQTT loop and disconnecting")
        mqtt.loop_stop()
        mqtt.disconnect()
        logger.info("Script terminated")

if __name__ == "__main__":
    main()
