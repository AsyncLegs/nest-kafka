import { SetMetadata } from '@nestjs/common';
import { KafkaSubscriberMetadataInterface } from '../interfaces';
import { KAFKA_CONSUMER } from '../constants';

export const KafkaConsumer = (topic: string): MethodDecorator => {
  return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
    SetMetadata<string, KafkaSubscriberMetadataInterface>(KAFKA_CONSUMER, {
      topic,
      groupId: topic,
      target: target.constructor.name,
      methodName: propertyKey,
      callback: descriptor.value,
    })(target, propertyKey, descriptor);
  };
};
