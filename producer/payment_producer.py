import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Connect to Kafka running on your Mac via Docker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Possible values for random generation
PAYMENT_METHODS = ['credit_card', 'debit_card', 'bank_transfer', 'digital_wallet']
STATUSES        = ['succeeded', 'failed', 'pending']
SOURCE_SYSTEMS  = ['mobile_ios', 'mobile_android', 'web_checkout', 'pos_terminal']
CURRENCIES      = ['USD', 'EUR', 'GBP']

def generate_payment_event():
    return {
        'payment_id':     'pay_' + fake.uuid4()[:8],
        'user_id':        'usr_' + fake.uuid4()[:8],
        'merchant_id':    'mer_' + fake.uuid4()[:8],
        'amount':         round(random.uniform(1.00, 999.99), 2),
        'currency':       random.choice(CURRENCIES),
        'payment_method': random.choice(PAYMENT_METHODS),
        'status':         random.choice(STATUSES),
        'timestamp':      datetime.now().isoformat(),
        'source_system':  random.choice(SOURCE_SYSTEMS)
    }

print("Payment producer started — sending events to Kafka...")
print("Press Ctrl+C to stop\n")

event_count = 0

while True:
    event = generate_payment_event()
    producer.send('payments.events', value=event)
    event_count += 1
    print(f"Event {event_count} sent: {event['payment_id']} | "
          f"{event['status']} | "
          f"${event['amount']} | "
          f"{event['source_system']}")
    time.sleep(1)