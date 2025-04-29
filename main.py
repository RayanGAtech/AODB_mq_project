import os
import logging
import threading
import time
import pymqi
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from pystray import Icon, Menu, MenuItem
from PIL import Image, ImageDraw
import html
import re

# === MQ Connection Settings ===
MQ_HOST = 'xx.xx.xxx.xx'           # IP Address
CHANNEL_NAME = 'xxx.xxxxx'        # Channel Name
QMGR_NAME = 'xxxxx'             # Queue Manager Name
MQ_PORT = 'xxxx'                 # Port Number
QUEUE_NAME = 'xxxxxxxx'              # Queue Name
USERNAME = None                     # Username (optional)
PASSWORD = None                     # Password (optional)


# Directories for Logs and XMLs
XML_SAVE_DIR = r"extracted_data"
LOG_DIR = r"logs"
os.makedirs(XML_SAVE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Logging Setup with rotation
LOG_FILE_PATH = os.path.join(LOG_DIR, "mq_listener.log")
log_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5 * 1024 * 1024, backupCount=3)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        log_handler,
        logging.StreamHandler()
    ]
)

# Global Stop Signal to control graceful shutdown
stop_signal = threading.Event()

# System Tray Setup
def create_image():
    img = Image.new('RGB', (64, 64), color="blue")
    d = ImageDraw.Draw(img)
    d.text((10, 20), "MQ", fill="white")
    return img

def on_exit(icon, item):
    logging.info("Exit clicked. Shutting down listener.")
    stop_signal.set()
    icon.stop()

def setup_tray():
    icon = Icon(
        "MQ Listener",
        icon=create_image(),
        menu=Menu(MenuItem('Exit', on_exit))
    )
    icon.run()

# Clean the message before saving (remove non-printable characters)
def clean_message(msg: str) -> str:
    # Remove non-printable/control characters (ASCII 0-31 except \n, \r, \t)
    msg = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', msg)

    # OPTIONAL: Remove RFH header if present
    if msg.startswith("RFH"):
        idx = msg.find('<?xml')
        if idx != -1:
            msg = msg[idx:]  # Keep only the actual XML part

    return msg

# Save Message as Clean XML with Timestamp
def save_message(msg: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = os.path.join(XML_SAVE_DIR, f"message_{timestamp}.xml")

    # Clean the message
    clean_msg = clean_message(msg)

    # Decode the HTML entities in the message content
    clean_content = html.unescape(clean_msg)

    # Create XML structure
    root = ET.Element("MQMessage")
    ET.SubElement(root, "Timestamp").text = timestamp
    ET.SubElement(root, "QueueName").text = QUEUE_NAME
    ET.SubElement(root, "MessageType").text = "Text"  # You can modify this based on your message type

    # Use the cleaned content as the text
    content_element = ET.SubElement(root, "Content")
    content_element.text = clean_content

    # Write the XML to file
    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)

    logging.info(f"Saved clean message to {filename}")

# MQ Connection Function
def connect_to_mq():
    try:
        conn_info = f"{MQ_HOST}({MQ_PORT})"
        qmgr = pymqi.QueueManager(None)

        qmgr.connect_tcp_client(
            QMGR_NAME,
            pymqi.CD(),
            CHANNEL_NAME,
            conn_info,
            USERNAME,
            PASSWORD
        )

        logging.info("Connected successfully to MQ.")
        return qmgr

    except pymqi.MQMIError as e:
        logging.error(f"Failed to connect to MQ: {e}")
        raise

    except Exception as e:
        logging.error(f"General error during MQ connection: {e}")
        raise

# MQ Listener to continuously fetch messages
def listen_to_queue():
    while not stop_signal.is_set():
        try:
            qmgr = connect_to_mq()
            queue = pymqi.Queue(qmgr, QUEUE_NAME)

            # Setup Get Message Options (GMO)
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_WAIT
            gmo.WaitInterval = 10000  # 10 seconds wait (in milliseconds)

            while not stop_signal.is_set():
                try:
                    msg = queue.get(None, None, gmo)
                    if msg:
                        logging.info("Received a message.")
                        
                        # Handle byte-string messages (e.g., RFH header or binary content)
                        if isinstance(msg, bytes):
                            msg = msg.decode('utf-8', errors='ignore')  # Decode bytes to string (ignore errors)

                        save_message(msg)
                        time.sleep(10)  # Wait for 10 seconds before processing the next message
                    else:
                        logging.info("No more messages available.")
                        stop_signal.set()  # Stop the listener after no messages are available
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        logging.info("No messages available, retrying...")
                        continue  # No message, keep waiting
                    else:
                        logging.error(f"Error receiving message: {e}")
                        break  # Break inner loop to reconnect

        except Exception as e:
            logging.error(f"Listener error: {e}")

        logging.info("Retrying connection in 10 seconds...")
        time.sleep(10)

# Main Entrypoint
if __name__ == "__main__":
    tray_thread = threading.Thread(target=setup_tray)
    tray_thread.daemon = True
    tray_thread.start()

    listen_to_queue()

    logging.info("Listener stopped.")