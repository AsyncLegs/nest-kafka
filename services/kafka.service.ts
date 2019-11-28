import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  Logger,
} from '@nestjs/common';
import nanoid from 'nanoid';
import { Kafka, Producer, Message, Consumer } from 'kafkajs';
import { ConfigService } from '../../config/config.service';
import { EventInterface } from '../../eventbus/interfaces';
import { inspect, isObject, isString } from 'util';

export interface ConsumersMap {
  groupId: string;
  consumer: EdgeConsumer;
}

interface EdgeConsumer extends Consumer {
  isHealthy: () => Promise<boolean>;
  lastHeartbeat: number;
}
@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private kafkaClient: Kafka;
  private kafkaProducer: Producer;
  private consumers = new Array<ConsumersMap>();
  private readonly logger: Logger;

  constructor(private readonly config: ConfigService) {
    this.logger = new Logger(KafkaService.name);
  }

  onModuleInit() {
    this.kafkaClient = new Kafka(this.config.getKafkaConfiguration());
    this.initializeProducer();
    this.logger.log(`Kafka integration service is initialized`);
  }
  onModuleDestroy() {
    this.logger.log(`Kafka integration service is destroying`);
  }
  async send(event: EventInterface) {
    try {
      const preparedMessage = this.createMessage(event);
      await this.kafkaProducer.send({
        topic: event.topic,
        messages: [preparedMessage],
      });
    } catch (e) {
      this.logger.error(`Kafka message sending error: ${inspect(e)}`);
    }
  }
  private createMessage(event: EventInterface): Message {
    let injectMeta = null;
    const preparedValue =
      isString(event.payload) || isObject(event.payload)
        ? JSON.stringify(event.payload)
        : event.payload;
    try {
      injectMeta = JSON.parse(preparedValue);
      injectMeta = {
        ...injectMeta,
        contractId: event.contractId,
      };
    } catch (e) {
      this.logger.error(`Failed to inject meta: ${e.message}`);
      throw e;
    }

    return {
      key: Buffer.from(nanoid()),
      value: Buffer.from(JSON.stringify(injectMeta)),
      headers: event.extra,
    };
  }
  public deserialize({ key, value, headers }: any) {
    let parsedValue = value.toString();
    try {
      parsedValue = JSON.parse(value.toString());
    } catch (e) {
      this.logger.error(
        `Failed to parse message value: ${parsedValue} from Kafka: ${e}`,
      );
    }
    return {
      key,
      value: parsedValue,
      headers: this.deserializeHeaders(headers),
    };
  }
  private deserializeHeaders(headers: any) {
    for (const [key, value] of Object.entries(headers)) {
      headers[key] = value.toString();
    }

    return headers;
  }

  initializeProducer(): void {
    this.kafkaProducer = this.kafkaClient.producer();
    this.kafkaProducer.on(this.kafkaProducer.events.CONNECT, e => {
      this.logger.log(`Kafka producer is connected now. ${e.timestamp}`);
    });
    this.kafkaProducer.on(this.kafkaProducer.events.DISCONNECT, e => {
      this.logger.log(`Kafka producer is disconnected now. ${e.timestamp}`);
    });
  }

  async getConsumer(groupId: string): Promise<Consumer> {
    const consumer = this.kafkaClient.consumer({
      groupId,
    });
    const { HEARTBEAT } = consumer.events;
    consumer['lastHeartbeat'] = 0;
    consumer.on(
      HEARTBEAT,
      ({ timestamp }) => (consumer['lastHeartbeat'] = timestamp),
    );
    consumer['isHealthy'] = async function() {
      const SESSION_TIMEOUT = 30000;
      // Consumer has heartbeat within the session timeout,
      // so it is healthy
      if (Date.now() - this.lastHeartbeat < SESSION_TIMEOUT) {
        return true;
      }
      // Consumer has not heartbeat, but maybe it's because the group is currently rebalancing
      try {
        const { state } = await consumer.describeGroup();
        return ['CompletingRebalance', 'PreparingRebalance'].includes(state);
      } catch (e) {
        return false;
      }
    };
    this.consumers.push({ groupId, consumer: consumer as EdgeConsumer });
    return consumer;
  }

  async areAllConsumersHealthy() {
    return this.consumers.every(
      async consumer => await consumer.consumer.isHealthy(),
    );
  }
}
