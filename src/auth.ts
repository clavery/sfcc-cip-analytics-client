export interface AuthConfig {
  clientId: string;
  clientSecret: string;
  instance: string;
}

export function getAuthConfig(): AuthConfig {
  const clientId = process.env.SFCC_CLIENT_ID;
  const clientSecret = process.env.SFCC_CLIENT_SECRET;
  const instance = process.env.SFCC_CIP_INSTANCE;

  if (!clientId) {
    throw new Error('SFCC_CLIENT_ID environment variable is not set');
  }
  if (!clientSecret) {
    throw new Error('SFCC_CLIENT_SECRET environment variable is not set');
  }
  if (!instance) {
    throw new Error('SFCC_CIP_INSTANCE environment variable is not set');
  }

  return {
    clientId,
    clientSecret,
    instance
  };
}

export async function getAccessToken(): Promise<string> {
  const config = getAuthConfig();
  
  const tokenUrl = `https://account.demandware.com/dwsso/oauth2/access_token?scope=SALESFORCE_COMMERCE_API:${config.instance}`;
  
  const response = await fetch(tokenUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`
    },
    body: 'grant_type=client_credentials'
  });
  
  if (!response.ok) {
    throw new Error(`Failed to get access token: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json() as any;
  
  if (!data.access_token) {
    throw new Error('No access token in response');
  }
  
  return data.access_token;
}

export function getAvaticaServerUrl(instance?: string): string {
  const targetInstance = instance || getAuthConfig().instance;
  return `https://jdbc.analytics.commercecloud.salesforce.com/${targetInstance}`;
}