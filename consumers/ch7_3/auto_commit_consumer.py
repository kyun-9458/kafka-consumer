from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import json


class AutoCommitConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'latest',              # offset을 파티션내에 가장 마지막부터 처리
                'enable.auto.commit': 'true',               # enable.auto.commit = 'true' 설정 시 MSG 유실 혹은 중복 실행 가능
                'auto.commit.interval.ms': '60000'         # 기본 값: 5000 (5초)
                }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt:  # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    auto_commit_consumer = AutoCommitConsumer('auto_commit_consumer')
    auto_commit_consumer.poll()