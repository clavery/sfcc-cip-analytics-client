export interface Logger {
  debug(message: string, ...args: any[]): void;
  info(message: string, ...args: any[]): void;
  warn(message: string, ...args: any[]): void;
  error(message: string, ...args: any[]): void;
}

export class DefaultLogger implements Logger {
  private readonly debugEnabled: boolean;

  constructor() {
    this.debugEnabled = process.env.SFCC_DEBUG === 'true' || process.env.SFCC_DEBUG === '1';
  }

  debug(message: string, ...args: any[]): void {
    if (this.debugEnabled) {
      console.debug(message, ...args);
    }
  }

  info(message: string, ...args: any[]): void {
    console.info(message, ...args);
  }

  warn(message: string, ...args: any[]): void {
    console.warn(message, ...args);
  }

  error(message: string, ...args: any[]): void {
    console.error(message, ...args);
  }
}

export class NoOpLogger implements Logger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
}

export const defaultLogger = new DefaultLogger();
