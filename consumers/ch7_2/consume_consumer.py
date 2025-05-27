from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time

class ConsumeConsumer(BaseConsumer): # MSG 리스트 형태로 여러건씩 가져오는 Consume 실습
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false'
                }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
                msg_lst = self.consumer.consume(num_messages=100) # 메시지를 리스트 형태로 여러건씩 가져올 때 consume
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst] # 기존 프로듀서의 로그는 binary형태로 그대로 가져오면 읽을 수 없음
                df = pd.DataFrame(msg_val_lst)
                print(df[:10]) # 10줄만


                self.logger.info(f'message 처리 로직 완료, Async Commit 후 2초 대기')
                # 로직 처리 완료 후 Async Commit 수행
                self.consumer.commit(asynchronous=True)
                self.logger.info(f'Commit 완료')
                time.sleep(2)

        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    consume_consumer= ConsumeConsumer('consume_consumer')
    consume_consumer.poll()