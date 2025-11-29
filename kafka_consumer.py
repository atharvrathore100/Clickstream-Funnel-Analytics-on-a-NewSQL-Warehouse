"""
Kafka consumer for Wikimedia pageviews JSON logs.

Usage (examples):
    python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews
    python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --group-id my-consumer-group
    python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --from-beginning
    python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 1000 --output output.jsonl
"""

import argparse
import json
import os
import signal
import sys
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError


def build_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: Optional[str] = None,
    auto_offset_reset: str = "latest",
    enable_auto_commit: bool = True,
    auto_commit_interval_ms: int = 1000,
    consumer_timeout_ms: int = 5000,
) -> KafkaConsumer:
    """
    Build and configure a Kafka consumer.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Topic to consume from
        group_id: Consumer group ID (None for no group)
        auto_offset_reset: Where to start if no offset stored ('earliest' or 'latest')
        enable_auto_commit: Whether to auto-commit offsets
        auto_commit_interval_ms: Interval for auto-commit in milliseconds
    
    Returns:
        Configured KafkaConsumer instance
    """
    consumer_config = {
        "bootstrap_servers": bootstrap_servers,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
        "key_deserializer": lambda m: m.decode("utf-8") if m else None,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "auto_commit_interval_ms": auto_commit_interval_ms,
        "consumer_timeout_ms": consumer_timeout_ms,  # Timeout when no messages available
    }
    
    if group_id:
        consumer_config["group_id"] = group_id
    
    return KafkaConsumer(topic, **consumer_config)


def consume_messages(
    consumer: KafkaConsumer,
    limit: Optional[int] = None,
    output_file: Optional[str] = None,
    verbose: bool = False,
) -> int:
    """
    Consume messages from Kafka topic.
    
    Args:
        consumer: KafkaConsumer instance
        limit: Maximum number of messages to consume (None for unlimited)
        output_file: Optional file path to write messages (JSONL format)
        verbose: Print detailed information about each message
    
    Returns:
        Number of messages consumed
    """
    consumed = 0
    output_fh = None
    
    if output_file:
        output_fh = open(output_file, "w", encoding="utf-8")
        if verbose:
            print(f"Writing messages to: {output_file}")
    
    try:
        for message in consumer:
            try:
                # Extract message data
                value = message.value
                key = message.key
                partition = message.partition
                offset = message.offset
                timestamp = message.timestamp
                
                if verbose:
                    print(f"\nMessage #{consumed + 1}:")
                    print(f"  Partition: {partition}")
                    print(f"  Offset: {offset}")
                    print(f"  Timestamp: {timestamp}")
                    print(f"  Key: {key}")
                    print(f"  Value: {json.dumps(value, indent=2)}")
                else:
                    # Simple progress indicator
                    if consumed % 100 == 0 and consumed > 0:
                        print(f"Consumed {consumed} messages...", end="\r")
                
                # Write to file if specified
                if output_fh:
                    record = {
                        "partition": partition,
                        "offset": offset,
                        "timestamp": timestamp,
                        "key": key,
                        "value": value,
                    }
                    output_fh.write(json.dumps(record) + "\n")
                    output_fh.flush()
                
                consumed += 1
                
                # Check limit
                if limit and consumed >= limit:
                    if verbose:
                        print(f"\nReached limit of {limit} messages")
                    break
                    
            except json.JSONDecodeError as e:
                print(f"\nError decoding message at partition {message.partition}, offset {message.offset}: {e}")
                continue
            except Exception as e:
                print(f"\nError processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        print(f"\n\nInterrupted by user. Consumed {consumed} messages.")
    except KafkaError as e:
        print(f"\nKafka error: {e}")
    finally:
        if output_fh:
            output_fh.close()
        consumer.close()
    
    return consumed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka consumer for Wikimedia pageviews JSON logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Consume from latest offset with a consumer group
  python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --group-id my-group
  
  # Consume from beginning (all messages)
  python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --from-beginning
  
  # Consume 100 messages and save to file
  python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 100 --output messages.jsonl
  
  # Verbose mode with manual commit
  python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --verbose --no-auto-commit
        """,
    )
    
    parser.add_argument(
        "--bootstrap",
        dest="bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        dest="topic",
        default=os.getenv("KAFKA_TOPIC", "wm_pageviews"),
        help="Topic to consume from (default: wm_pageviews)",
    )
    parser.add_argument(
        "--group-id",
        dest="group_id",
        default=os.getenv("KAFKA_GROUP_ID", "clickstream-consumer-group"),
        help="Consumer group ID (default: clickstream-consumer-group)",
    )
    parser.add_argument(
        "--from-beginning",
        dest="from_beginning",
        action="store_true",
        help="Start from earliest available offset (default: latest)",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Maximum number of messages to consume (default: unlimited)",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default=None,
        help="Output file path for messages (JSONL format)",
    )
    parser.add_argument(
        "--verbose",
        dest="verbose",
        action="store_true",
        help="Print detailed information about each message",
    )
    parser.add_argument(
        "--no-auto-commit",
        dest="no_auto_commit",
        action="store_true",
        help="Disable auto-commit of offsets (manual commit required)",
    )
    parser.add_argument(
        "--auto-commit-interval",
        dest="auto_commit_interval",
        type=int,
        default=1000,
        help="Auto-commit interval in milliseconds (default: 1000)",
    )
    parser.add_argument(
        "--timeout",
        dest="timeout",
        type=int,
        default=5000,
        help="Consumer timeout in milliseconds when no messages available (default: 5000)",
    )
    
    args = parser.parse_args()
    
    # Determine offset reset strategy
    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    
    # Handle consumer group
    group_id = args.group_id if args.group_id else None
    
    print(f"Connecting to Kafka: {args.bootstrap}")
    print(f"Topic: {args.topic}")
    if group_id:
        print(f"Consumer Group: {group_id}")
    else:
        print("Consumer Group: None (no group coordination)")
    print(f"Offset Reset: {auto_offset_reset}")
    print(f"Auto-commit: {not args.no_auto_commit}")
    if args.limit:
        print(f"Limit: {args.limit} messages")
    print("-" * 60)
    
    try:
        consumer = build_consumer(
            bootstrap_servers=args.bootstrap,
            topic=args.topic,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=not args.no_auto_commit,
            auto_commit_interval_ms=args.auto_commit_interval,
            consumer_timeout_ms=args.timeout,
        )
        
        # Get topic metadata
        partitions = consumer.partitions_for_topic(args.topic)
        if partitions:
            print(f"Topic has {len(partitions)} partition(s): {sorted(partitions)}")
        else:
            print(f"Warning: Topic '{args.topic}' not found or no partitions available")
        
        print("Starting to consume messages... (Press Ctrl+C to stop)\n")
        
        consumed = consume_messages(
            consumer=consumer,
            limit=args.limit,
            output_file=args.output,
            verbose=args.verbose,
        )
        
        print(f"\n{'='*60}")
        print(f"Total messages consumed: {consumed:,}")
        if args.output:
            print(f"Messages written to: {args.output}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n\nConsumer interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

