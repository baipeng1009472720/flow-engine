# Flow Engine

Standalone Maven reactor for the PlayRewards flow runtime engine.

- `engine-contract`: public runtime contracts
- `engine-spi`: extension interfaces
- `engine-core`: framework-independent execution core
- `engine-groovy-rules`: Groovy 脚本沙箱规则（白名单 + AST 检查），运行时编译与平台设计态静态检查共用，只依赖 Groovy
- `engine-adapters`: infrastructure adapters
- `engine-server`: executable Spring Boot server with `/actuator/health`

Run `mvn test`, or package the server with `mvn -pl engine-server -am package`.

## Runtime contract compatibility

| Contract surface | Accepted version | Unknown-field policy |
| --- | --- | --- |
| `ExecutableFlowBundle` | `1.0` only | Reject unknown structural fields; only nested `config` and JSON Schema documents remain extensible JSON. |
| `FlowDefinition` | `2.0` only | Nodes use only `id`, `componentCode`, `nodeCode`, `nodeName`, `config`; edges use semantic `sourcePort` and `targetPort`; unknown structural fields are rejected. |
| Component type | Current persisted vocabulary | `MERGE` and `SWITCH` are retained for historical JSON reading; a later compiler is responsible for rejecting unsupported execution. |

`bundleHash` is SHA-256 lowercase hex over canonical UTF-8 JSON: object keys are recursively sorted, array order is retained, and the root `bundleHash` plus `trust.signature` are excluded. JSON values are deeply copied into immutable maps and arrays. Sensitive execution inputs, scope values, context references, streamed payloads and trust signatures are redacted from `toString()`.
