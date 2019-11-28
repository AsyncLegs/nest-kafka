import { Module, OnModuleInit } from '@nestjs/common';
import { KafkaService } from './services/kafka.service';
import { KafkaExplorer } from './services/kafka.explorer';

@Module({
  providers: [KafkaExplorer, KafkaService],
  exports: [KafkaService],
})
export class KafkaModule implements OnModuleInit {
  constructor(private readonly explorer: KafkaExplorer) {}

  onModuleInit() {
    this.explorer.explore();
  }
}
