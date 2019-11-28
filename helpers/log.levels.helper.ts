import { logLevel } from 'kafkajs';

export const toWinstonLogLevel = (level: logLevel): string => {
  switch (level) {
    case logLevel.ERROR:
    case logLevel.NOTHING:
      return 'error';
    case logLevel.WARN:
      return 'warn';
    case logLevel.INFO:
      return 'info';
    case logLevel.DEBUG:
      return 'debug';
  }
};
export const toNestLogLevel = (level: logLevel): string => {
  switch (level) {
    case logLevel.ERROR:
    case logLevel.NOTHING:
      return 'error';
    case logLevel.WARN:
      return 'warn';
    case logLevel.INFO:
      return 'log';
    case logLevel.DEBUG:
      return 'debug';
  }
};
