import {
  Injectable as InjectableDecorator,
  Type,
  Logger,
} from '@nestjs/common';
import { ModulesContainer, Reflector } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { Module } from '@nestjs/core/injector/module';
import { Injectable } from '@nestjs/common/interfaces';
import { MetadataScanner } from '@nestjs/core/metadata-scanner';
import { KAFKA_CONSUMER } from '../constants';
import { inspect } from 'util';
import { Consumer } from 'kafkajs';
import { KafkaService } from './kafka.service';

@InjectableDecorator()
export class KafkaExplorer {
  private readonly logger: Logger;

  constructor(
    private readonly modulesContainer: ModulesContainer,
    private readonly reflector: Reflector,
    private readonly kafkaClient: KafkaService,
  ) {
    this.logger = new Logger(KafkaExplorer.name);
  }

  explore() {
    const providers = KafkaExplorer.getProviders([
      ...this.modulesContainer.values(),
    ]);

    providers.forEach((wrapper: InstanceWrapper) => {
      const { instance } = wrapper;
      if (instance) {
        new MetadataScanner().scanFromPrototype(
          instance,
          Object.getPrototypeOf(instance),
          (key: string) => {
            if (KafkaExplorer.isConsumer(instance[key], this.reflector)) {
              this.handleConsumer(
                instance,
                key,
                KafkaExplorer.getConsumerMetadata(
                  instance[key],
                  this.reflector,
                ),
              );
            }
          },
        );
      }
    });
  }

  private async handleConsumer(instance, key, options?) {
    const { topic, groupId } = options;
    const consumer = await this.getKafkaConsumer(topic, groupId);
    await consumer.run({
      eachMessage: instance[key].bind(instance),
    });
  }

  static isConsumer(
    target: Type<any>,
    reflector: Reflector = new Reflector(),
  ): boolean {
    return !!reflector.get(KAFKA_CONSUMER, target);
  }

  static getConsumerMetadata(
    target: Type<any>,
    reflector: Reflector = new Reflector(),
  ): any {
    return reflector.get(KAFKA_CONSUMER, target);
  }

  static getProviders(modules: Module[]): Array<InstanceWrapper<Injectable>> {
    return modules
      .filter(({ providers }) => providers.size > 0)
      .map((module: Module) => {
        return module.providers;
      })
      .reduce((acc, map) => {
        acc.push(...map.values());
        return acc;
      }, [])
      .filter((wrapper: InstanceWrapper) => {
        return wrapper.metatype;
      });
  }
  private async getKafkaConsumer(
    topic: string,
    groupId: string,
  ): Promise<Consumer> {
    const consumer = await this.kafkaClient.getConsumer(groupId);
    consumer.on(consumer.events.GROUP_JOIN, e => {
      this.logger.log(
        `Kafka consumer[${e.id}] has joined to topic: ${topic}, group: ${e.payload.groupId}.`,
      );
    });
    consumer.on(consumer.events.DISCONNECT, e => {
      this.logger.log(
        `Kafka consumer [${e.id}] has disconnected from topic: ${topic}, group: ${e.payload.groupId}`,
      );
    });
    await consumer.subscribe({ topic });
    await consumer.connect();

    return consumer;
  }
}
