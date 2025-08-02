import * as protobuf from "protobufjs";
import { v4 as uuidv4 } from "uuid";
import {
  IConnectionProperties,
  IWireMessage,
  IPrepareRequest,
  IPrepareResponse,
  IExecuteRequest,
  IExecuteResponse,
  IStatementHandle,
  ITypedValue,
  Rep,
} from "./protocol";
import {
  NormalizedExecuteResponse,
  NormalizedFetchResponse,
} from "./normalized-types";
import * as path from "path";
import { Logger, defaultLogger } from "./logger";
import Long from "long";

interface TokenInfo {
  accessToken: string;
  expiresAt: number; // Unix timestamp
}

export interface CIPClientOptions {
  logger?: Logger;
}

export class CIPClient {
  private readonly clientId: string;
  private readonly clientSecret: string;
  private readonly instance: string;
  private readonly serverUrl: string;
  private readonly logger: Logger;
  private root: protobuf.Root | null = null;
  private connectionId: string | null = null;
  private tokenInfo: TokenInfo | null = null;
  private sessionId: string | undefined;

  constructor(
    clientId: string,
    clientSecret: string,
    instance: string,
    options: CIPClientOptions = {},
  ) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.instance = instance;
    this.serverUrl = `https://jdbc.analytics.commercecloud.salesforce.com/${instance}`;
    this.logger = options.logger || defaultLogger;
  }

  /**
   * Initializes the protobuf schemas. Called automatically on first request.
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.root) {
      // TODO: use static build
      const protoFiles = [
        path.join(__dirname, "../proto/common.proto"),
        path.join(__dirname, "../proto/requests.proto"),
        path.join(__dirname, "../proto/responses.proto"),
      ];
      this.root = await protobuf.load(protoFiles);
    }
  }

  /**
   * Ensures we have a valid access token, refreshing if necessary.
   */
  private async ensureValidToken(): Promise<void> {
    const now = Date.now();

    // Check if we need to get a new token
    if (!this.tokenInfo || now >= this.tokenInfo.expiresAt) {
      this.logger.debug("Refreshing access token...");

      const tokenUrl = `https://account.demandware.com/dwsso/oauth2/access_token?scope=SALESFORCE_COMMERCE_API:${this.instance}`;

      const response = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(`${this.clientId}:${this.clientSecret}`).toString("base64")}`,
        },
        body: "grant_type=client_credentials",
      });

      if (!response.ok) {
        throw new Error(
          `Failed to get access token: ${response.status} ${response.statusText}`,
        );
      }

      const data = (await response.json()) as any;

      if (!data.access_token) {
        throw new Error("No access token in response");
      }

      // Calculate expiration time (subtract 60 seconds as buffer)
      const expiresIn = data.expires_in || 3600; // Default to 1 hour if not provided
      const expiresAt = now + (expiresIn - 60) * 1000;

      this.tokenInfo = {
        accessToken: data.access_token,
        expiresAt,
      };

      this.logger.debug(
        "Access token refreshed, expires at:",
        new Date(expiresAt).toISOString(),
      );
    }
  }

  /**
   * Opens a new connection to the Avatica server.
   * @param info Connection properties (e.g., user, password, schema).
   */
  public async openConnection(info: IConnectionProperties = {}): Promise<void> {
    if (this.connectionId) {
      throw new Error(
        "Connection already open. Close the existing connection first.",
      );
    }

    const connectionId = uuidv4(); // Use provided ID or generate a new one
    const requestPayload = {
      connectionId,
      info,
    };
    const response = await this.sendRequest(
      "OpenConnectionRequest",
      requestPayload,
    );
    // The server returns its own connectionId in the response, which we should use.
    this.connectionId = response.connectionId || connectionId;
  }

  /**
   * Closes the existing connection.
   */
  public async closeConnection(): Promise<void> {
    if (!this.connectionId) {
      throw new Error("No connection to close.");
    }
    await this.sendRequest("CloseConnectionRequest", {
      connectionId: this.connectionId,
    });
    this.connectionId = null;
  }

  /**
   * Creates a new statement for the current connection.
   * @returns The ID of the newly created statement.
   */
  public async createStatement(): Promise<number> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }
    const response = await this.sendRequest("CreateStatementRequest", {
      connectionId: this.connectionId,
    });
    return response.statementId;
  }

  /**
   * Closes an existing statement.
   * @param statementId The ID of the statement to close.
   */
  public async closeStatement(statementId: number): Promise<void> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }
    await this.sendRequest("CloseStatementRequest", {
      connectionId: this.connectionId,
      statementId,
    });
  }

  /**
   * Prepares and executes a SQL query in a single step.
   * @param statementId The ID of the statement.
   * @param sql The SQL query to execute.
   * @param maxRowCount The maximum number of rows to return in the first frame (-1 for all).
   * @returns The full execution response, including the first frame of results with normalized offset values.
   */
  public async execute(
    statementId: number,
    sql: string,
    maxRowCount: number = -1,
  ): Promise<NormalizedExecuteResponse> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }
    const requestPayload = {
      connectionId: this.connectionId,
      statementId,
      sql,
      maxRowCount,
      firstFrameMaxSize: maxRowCount,
    };
    // The actual request type is PrepareAndExecuteRequest
    const response = await this.sendRequest(
      "PrepareAndExecuteRequest",
      requestPayload,
    );

    // Normalize frame data in all results
    if (response.results) {
      response.results.forEach((result: any) => {
        if (result.firstFrame) {
          result.firstFrame = this.normalizeFrame(result.firstFrame);
        }
      });
    }

    return response as NormalizedExecuteResponse;
  }

  /**
   * Fetches the next frame of results for a query.
   * @param statementId The statement ID.
   * @param offset The starting row offset for the new frame.
   * @param fetchMaxRowCount The maximum number of rows for this frame.
   * @returns The fetch response containing the next frame with normalized offset values.
   */
  public async fetch(
    statementId: number,
    offset: number,
    fetchMaxRowCount: number,
  ): Promise<NormalizedFetchResponse> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }
    const requestPayload = {
      connectionId: this.connectionId,
      statementId,
      offset,
      fetchMaxRowCount,
    };
    const response = await this.sendRequest("FetchRequest", requestPayload);

    // Normalize the frame data
    if (response.frame) {
      response.frame = this.normalizeFrame(response.frame);
    }

    return response;
  }

  /**
   * Prepares a SQL statement and returns metadata including parameter information.
   * @param sql The SQL query to prepare (with ? placeholders for parameters).
   * @param maxRowsTotal The maximum number of rows that will be allowed for this query (-1 for no limit).
   * @returns The prepare response containing statement metadata.
   */
  public async prepare(
    sql: string,
    maxRowsTotal: number = -1,
  ): Promise<IPrepareResponse> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }

    const requestPayload: IPrepareRequest = {
      connectionId: this.connectionId,
      sql,
      maxRowsTotal,
    };

    const response = await this.sendRequest("PrepareRequest", requestPayload);
    return response as IPrepareResponse;
  }

  /**
   * Executes a prepared statement with parameters.
   * @param statementHandle The statement handle returned from prepare().
   * @param parameters Array of parameter values to bind to the prepared statement.
   * @param firstFrameMaxSize The maximum number of rows to return in the first frame (-1 for no limit).
   * @returns The execution response containing results with normalized offset values.
   */
  public async executeWithParameters(
    statementHandle: IStatementHandle,
    parameters: any[] = [],
    firstFrameMaxSize: number = -1,
  ): Promise<IExecuteResponse> {
    if (!this.connectionId) {
      throw new Error("No connection available. Call openConnection() first.");
    }

    // Convert JavaScript values to TypedValues
    var parameterValues: ITypedValue[] = parameters.map((param) =>
      this.createTypedValue(param),
    );

    const requestPayload: IExecuteRequest = {
      statementHandle,
      parameterValues,
      hasParameterValues: parameterValues.length > 0,
      firstFrameMaxSize,
    };

    const response = await this.sendRequest("ExecuteRequest", requestPayload);

    // Normalize frame data in all results
    if (response.results) {
      response.results.forEach((result: any) => {
        if (result.firstFrame) {
          result.firstFrame = this.normalizeFrame(result.firstFrame);
        }
      });
    }

    return response as IExecuteResponse;
  }

  /**
   * Convenience method to prepare and execute a statement with parameters in one call.
   * @param sql The SQL query with ? placeholders.
   * @param parameters Array of parameter values.
   * @param firstFrameMaxSize The maximum number of rows to return in the first frame.
   * @returns The execution response containing results.
   */
  public async prepareAndExecuteWithParameters(
    sql: string,
    parameters: any[] = [],
    firstFrameMaxSize: number = -1,
  ): Promise<IExecuteResponse> {
    const prepareResponse = await this.prepare(sql);
    if (!prepareResponse.statement) {
      throw new Error(
        "Failed to prepare statement - no statement handle returned",
      );
    }

    return this.executeWithParameters(
      prepareResponse.statement,
      parameters,
      firstFrameMaxSize,
    );
  }

  /**
   * A generic method to serialize, send, and deserialize Avatica messages.
   * @param requestTypeName The short name of the request message type (e.g., "OpenConnectionRequest").
   * @param payload The JavaScript object for the request payload.
   * @returns A promise that resolves to the deserialized response payload.
   */
  private async sendRequest(
    requestTypeName: string,
    payload: object,
  ): Promise<any> {
    // Ensure we're initialized and have a valid token
    await this.ensureInitialized();
    await this.ensureValidToken();

    // 1. Construct the fully-qualified Java class name for the request.
    // This is a convention of the Avatica Protobuf wire format.
    const fullRequestClassName = `org.apache.calcite.avatica.proto.Requests$${requestTypeName}`;

    // 2. Serialize the specific request message (e.g., OpenConnectionRequest).
    const RequestType = this.root!.lookupType(requestTypeName);
    const requestMessage = RequestType.create(payload);
    const serializedRequest = RequestType.encode(requestMessage).finish();

    // 3. Wrap the serialized request in a WireMessage.
    const WireMessage = this.root!.lookupType("WireMessage");
    const wirePayload = {
      name: fullRequestClassName,
      wrappedMessage: serializedRequest,
    };
    const wireMessage = WireMessage.create(wirePayload);
    const serializedWireMessage = WireMessage.encode(wireMessage).finish();

    this.logger.debug(`Sending request: ${requestTypeName}`, {
      serverUrl: this.serverUrl,
      instance: this.instance,
      requestPayload: payload,
      requestClassName: fullRequestClassName,
      serializedLength: serializedWireMessage.length,
    });
    // 4. Send the request using fetch.
    const httpResponse = await fetch(this.serverUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf",
        "X-Client-Version": "2.11.0",
        InstanceId: this.instance,
        Authorization: `Bearer ${this.tokenInfo!.accessToken}`,
        "x-session-id": this.sessionId || undefined,
      },
      body: serializedWireMessage,
    });

    // if the response has an x-session-id header, log it and store in this class
    const sessionId = httpResponse.headers.get("x-session-id");
    if (sessionId) {
      this.logger.debug(`Session ID: ${sessionId}`);
      this.sessionId = sessionId;
    }

    if (!httpResponse.ok) {
      const errorText = await httpResponse.text();
      throw new Error(
        `Avatica server error: ${httpResponse.status} ${httpResponse.statusText} - ${errorText}`,
      );
    }

    // 5. Decode the response WireMessage.
    const responseBuffer = await httpResponse.arrayBuffer();
    const responseWireMessage = WireMessage.decode(
      new Uint8Array(responseBuffer),
    );

    // 6. Find the corresponding response type and decode the wrapped message.
    // The response class name is derived from the response WireMessage's `name` field.
    const fullResponseClassName = (responseWireMessage as any).name;
    const responseTypeName = fullResponseClassName.split("$").pop()!;

    // Handle ErrorResponse specifically
    if (responseTypeName === "ErrorResponse") {
      const ErrorResponseType = this.root!.lookupType(responseTypeName);
      const errorDetails = ErrorResponseType.decode(
        (responseWireMessage as any).wrappedMessage,
      ) as any;
      throw new Error(
        `Avatica Error: ${errorDetails.errorMessage} (SQLState: ${errorDetails.sqlState}, ErrorCode: ${errorDetails.errorCode})`,
      );
    }

    const ResponseType = this.root!.lookupType(responseTypeName);
    const decodedResponse = ResponseType.decode(
      (responseWireMessage as any).wrappedMessage,
    );
    this.logger.debug(`Received response: ${responseTypeName}`, {
      responseTypeName,
      responseClassName: fullResponseClassName,
      responsePayload: decodedResponse.toJSON(),
      responseLength: (responseWireMessage as any).wrappedMessage.length,
    });
    return decodedResponse;
  }

  /**
   * Normalizes protobuf Long values to JavaScript numbers.
   * @param value The value that might be a protobuf Long
   * @returns A JavaScript number
   */
  private normalizeLongValue(value: any): number {
    if (value && typeof value === "object" && "toNumber" in value) {
      return (value as any).toNumber();
    }
    return Number(value || 0);
  }

  /**
   * Normalizes frame data to ensure offset and other numeric fields are plain numbers.
   * @param frame The frame object to normalize
   * @returns The frame with normalized numeric values
   */
  private normalizeFrame(frame: any): any {
    if (!frame) return frame;

    return {
      ...frame,
      offset: this.normalizeLongValue(frame.offset),
      rows: frame.rows || [],
    };
  }

  /**
   * Converts JavaScript values to Avatica TypedValue format for prepared statement parameters.
   * @param value The JavaScript value to convert
   * @returns A TypedValue object suitable for Avatica protocol
   */
  private createTypedValue(value: any): ITypedValue {
    if (value === null || value === undefined) {
      return { type: Rep.NULL, null: true };
    }

    if (typeof value === "string") {
      return { type: Rep.STRING, stringValue: value };
    }

    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        return { type: Rep.LONG, numberValue: value };
      } else {
        return { type: Rep.DOUBLE, doubleValue: value };
      }
    }

    if (typeof value === "boolean") {
      return { type: Rep.BOOLEAN, boolValue: value };
    }

    if (value instanceof Date) {
      // Convert Date to timestamp (milliseconds since epoch)
      return { type: Rep.JAVA_SQL_DATE, numberValue: value.getTime() };
    }

    if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
      return {
        type: Rep.BYTE_STRING,
        bytesValue: value instanceof Uint8Array ? value : new Uint8Array(value),
      };
    }

    // For arrays
    if (Array.isArray(value)) {
      const arrayValues = value.map((v) => this.createTypedValue(v));
      return {
        type: Rep.ARRAY,
        arrayValue: arrayValues,
        componentType:
          arrayValues.length > 0 ? arrayValues[0].type : Rep.OBJECT,
      };
    }

    // Fallback to string representation for complex objects
    return { type: Rep.STRING, stringValue: String(value) };
  }
}
