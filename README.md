# Flow Engine

Standalone Maven reactor for the PlayRewards flow runtime engine.

- `engine-contract`: public runtime contracts
- `engine-spi`: extension interfaces
- `engine-core`: framework-independent execution core
- `engine-adapters`: infrastructure adapters
- `engine-server`: executable Spring Boot server with `/actuator/health`

Run `mvn test`, or package the server with `mvn -pl engine-server -am package`.
