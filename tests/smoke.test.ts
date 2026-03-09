import { CIPClient, FetchFunction } from '../src/cip-client';
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

  it('should accept a custom fetch function', () => {
    const mockFetch: FetchFunction = jest.fn();
    const client = new CIPClient(
      'test-client-id',
      'test-client-secret',
      'test-instance',
      { fetch: mockFetch }
    );
    expect(client).toBeInstanceOf(CIPClient);
  });

  it('should use custom fetch when opening connection', async () => {
    const mockFetch: FetchFunction = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: () => Promise.resolve({ access_token: 'test-token', expires_in: 3600 }),
      text: () => Promise.resolve(''),
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
      headers: { get: () => null },
    });

    const client = new CIPClient(
      'test-client-id',
      'test-client-secret',
      'test-instance',
      { fetch: mockFetch }
    );

    // openConnection will call fetch for token, then fail on protobuf request
    // but we can verify fetch was called with the token URL
    await expect(client.openConnection()).rejects.toThrow();

    expect(mockFetch).toHaveBeenCalled();
    const firstCall = (mockFetch as jest.Mock).mock.calls[0];
    expect(firstCall[0]).toContain('account.demandware.com');
    expect(firstCall[1].method).toBe('POST');
  });

  it('should use custom fetch in getAccessToken', async () => {
    const mockFetch: auth.FetchFunction = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: () => Promise.resolve({ access_token: 'mock-token', expires_in: 3600 }),
    });

    // Set required env vars for getAccessToken
    const originalEnv = { ...process.env };
    process.env.SFCC_CLIENT_ID = 'test-id';
    process.env.SFCC_CLIENT_SECRET = 'test-secret';
    process.env.SFCC_CIP_INSTANCE = 'test-instance';

    try {
      const token = await auth.getAccessToken({ fetch: mockFetch });
      expect(token).toBe('mock-token');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    } finally {
      process.env = originalEnv;
    }
  });
});