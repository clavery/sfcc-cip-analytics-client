import { CIPClient } from '../src/cip-client';
import * as auth from '../src/auth';
import * as types from '../src/data/types';
import * as protocol from '../src/protocol';

describe('Smoke Tests', () => {
  it('should export CIPClient class', () => {
    expect(CIPClient).toBeDefined();
    expect(typeof CIPClient).toBe('function');
  });

  it('should export auth functions', () => {
    expect(auth).toBeDefined();
    expect(typeof auth.getAuthConfig).toBe('function');
    expect(typeof auth.getAccessToken).toBe('function');
  });

  it('should export data types', () => {
    expect(types).toBeDefined();
    expect(typeof types).toBe('object');
  });

  it('should export protocol', () => {
    expect(protocol).toBeDefined();
    expect(typeof protocol).toBe('object');
  });

  it('should create a CIPClient instance', () => {
    const client = new CIPClient(
      'test-client-id',
      'test-client-secret',
      'test-instance'
    );
    expect(client).toBeInstanceOf(CIPClient);
  });
});