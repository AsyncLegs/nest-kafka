import { EachMessagePayload } from 'kafkajs';

export interface KafkaSubscriberMetadataInterface {
  topic: string;
  groupId?: string;
  target: string;
  methodName: string;
  callback: (payload: EachMessagePayload) => Promise<void>;
}
