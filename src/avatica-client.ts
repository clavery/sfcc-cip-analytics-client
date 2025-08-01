// src/client.ts

import * as protobuf from 'protobufjs';
import { v4 as uuidv4 } from 'uuid'; // For generating connection IDs
import { IConnectionProperties, IExecuteResponse, IWireMessage } from './protocol';

export class AvaticaProtobufClient {
  private readonly serverUrl: string;
  private root: protobuf.Root;
  private connectionId: string | null = null;
    instance: string;
    accessToken: string | undefined;
    sessionId: string | undefined;

  private constructor(serverUrl: string, instance: string, root: protobuf.Root, accessToken?: string) {
    this.serverUrl = serverUrl;
    this.root = root;
    this.instance = instance;
    this.accessToken = accessToken;
  }

  /**
   * Asynchronously creates and initializes an AvaticaProtobufClient.
   * @param serverUrl The URL of the Avatica server
   * @param protoFiles An array of paths to the .proto files.
   * @returns A promise that resolves to a new AvaticaProtobufClient instance.
   */
  public static async create(serverUrl: string, instance: string, protoFiles: string[], accessToken: string): Promise<AvaticaProtobufClient> {
    const root = await protobuf.load(protoFiles);
    return new AvaticaProtobufClient(serverUrl, instance, root, accessToken);
  }

  /**
   * Opens a new connection to the Avatica server.
   * @param info Connection properties (e.g., user, password, schema).
   */
  public async openConnection(info: IConnectionProperties = {}): Promise<void> {
    if (this.connectionId) {
      throw new Error('Connection already open. Close the existing connection first.');
    }

    const connectionId = uuidv4(); // Use provided ID or generate a new one
    const requestPayload = {
      connectionId,
      info,
    };
    const response = await this.sendRequest('OpenConnectionRequest', requestPayload);
    // The server returns its own connectionId in the response, which we should use.
    this.connectionId = response.connectionId || connectionId;
  }

  /**
   * Closes the existing connection.
   */
  public async closeConnection(): Promise<void> {
    if (!this.connectionId) {
      throw new Error('No connection to close.');
    }
    await this.sendRequest('CloseConnectionRequest', { connectionId: this.connectionId });
    this.connectionId = null;
  }

  /**
   * Creates a new statement for the current connection.
   * @returns The ID of the newly created statement.
   */
  public async createStatement(): Promise<number> {
    if (!this.connectionId) {
      throw new Error('No connection available. Call openConnection() first.');
    }
    const response = await this.sendRequest('CreateStatementRequest', { connectionId: this.connectionId });
    return response.statementId;
  }

  /**
   * Closes an existing statement.
   * @param statementId The ID of the statement to close.
   */
  public async closeStatement(statementId: number): Promise<void> {
    if (!this.connectionId) {
      throw new Error('No connection available. Call openConnection() first.');
    }
    await this.sendRequest('CloseStatementRequest', { connectionId: this.connectionId, statementId });
  }

  /**
   * Prepares and executes a SQL query in a single step.
   * @param statementId The ID of the statement.
   * @param sql The SQL query to execute.
   * @param maxRowCount The maximum number of rows to return in the first frame (-1 for all).
   * @returns The full execution response, including the first frame of results.
   */
  public async execute(statementId: number, sql: string, maxRowCount: number = -1): Promise<IExecuteResponse> {
    if (!this.connectionId) {
      throw new Error('No connection available. Call openConnection() first.');
    }
    const requestPayload = {
      connectionId: this.connectionId,
      statementId,
      sql,
      maxRowCount,
    };
    // The actual request type is PrepareAndExecuteRequest
    const response = await this.sendRequest('PrepareAndExecuteRequest', requestPayload);
    return response as IExecuteResponse;
  }

  /**
   * Fetches the next frame of results for a query.
   * @param statementId The statement ID.
   * @param offset The starting row offset for the new frame.
   * @param fetchMaxRowCount The maximum number of rows for this frame.
   * @returns The fetch response containing the next frame.
   */
  public async fetch(statementId: number, offset: number, fetchMaxRowCount: number) {
      if (!this.connectionId) {
        throw new Error('No connection available. Call openConnection() first.');
      }
      const requestPayload = {
          connectionId: this.connectionId,
          statementId,
          offset,
          fetchMaxRowCount
      };
      return await this.sendRequest('FetchRequest', requestPayload);
  }

  /**
   * A generic method to serialize, send, and deserialize Avatica messages.
   * @param requestTypeName The short name of the request message type (e.g., "OpenConnectionRequest").
   * @param payload The JavaScript object for the request payload.
   * @returns A promise that resolves to the deserialized response payload.
   */
  private async sendRequest(requestTypeName: string, payload: object): Promise<any> {
    // 1. Construct the fully-qualified Java class name for the request.
    // This is a convention of the Avatica Protobuf wire format.
    const fullRequestClassName = `org.apache.calcite.avatica.proto.Requests$${requestTypeName}`;

    // 2. Serialize the specific request message (e.g., OpenConnectionRequest).
    const RequestType = this.root.lookupType(requestTypeName);
    const requestMessage = RequestType.create(payload);
    const serializedRequest = RequestType.encode(requestMessage).finish();

    // 3. Wrap the serialized request in a WireMessage.
    const WireMessage = this.root.lookupType('WireMessage');
    const wirePayload = {
      name: fullRequestClassName,
      wrappedMessage: serializedRequest,
    };
    const wireMessage = WireMessage.create(wirePayload);
    const serializedWireMessage = WireMessage.encode(wireMessage).finish();

    console.debug(`Sending request: ${requestTypeName}`, {
      serverUrl: this.serverUrl,
      instance: this.instance,
      requestPayload: payload,
      requestClassName: fullRequestClassName,
      serializedLength: serializedWireMessage.length,
    });
    // 4. Send the request using fetch.
    const httpResponse = await fetch(this.serverUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-protobuf',
        'X-Client-Version': '2.11.0', // Example client version, adjust as needed
        'InstanceId': this.instance,
        'Authorization': this.accessToken ? `Bearer ${this.accessToken}` : '',
        'x-session-id': this.sessionId || undefined
      },
      body: serializedWireMessage,
    });

    // if the response has an x-session-id header, log it and store in this class
    const sessionId = httpResponse.headers.get('x-session-id');
    if (sessionId) {
      console.debug(`Session ID: ${sessionId}`);
      this.sessionId = sessionId;
    }

    if (!httpResponse.ok) {
      const errorText = await httpResponse.text();
      throw new Error(`Avatica server error: ${httpResponse.status} ${httpResponse.statusText} - ${errorText}`);
    }

    // 5. Decode the response WireMessage.
    const responseBuffer = await httpResponse.arrayBuffer();
    const responseWireMessage = WireMessage.decode(new Uint8Array(responseBuffer));

    // 6. Find the corresponding response type and decode the wrapped message.
    // The response class name is derived from the response WireMessage's `name` field.
    const fullResponseClassName = responseWireMessage.name;
    const responseTypeName = fullResponseClassName.split('$').pop()!;
    
    // Handle ErrorResponse specifically
    if (responseTypeName === 'ErrorResponse') {
        const ErrorResponseType = this.root.lookupType(responseTypeName);
        const errorDetails = ErrorResponseType.decode(responseWireMessage.wrappedMessage);
        throw new Error(`Avatica Error: ${errorDetails.errorMessage} (SQLState: ${errorDetails.sqlState}, ErrorCode: ${errorDetails.errorCode})`);
    }

    const ResponseType = this.root.lookupType(responseTypeName);
    const decodedResponse = ResponseType.decode(responseWireMessage.wrappedMessage);
    console.debug(`Received response: ${responseTypeName}`, {
      responseTypeName,
      responseClassName: fullResponseClassName,
      responsePayload: decodedResponse.toJSON(),
      responseLength: responseWireMessage.wrappedMessage.length,
    });
    return decodedResponse;

  }
}
