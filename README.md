# Redis Multi-tenant Queue

## Building
The following properties are required to build this project:
* nexusUsername
* nexusPassword

They can be specified in a file called `gradle.properties` in this project's root directory
```
nexusUsername=my_username
nexusPassword=my_password
```

Or they can be specified as command line arguments e.g.
`./gradlew clean build -PnexusUsername=my_username -PnexusPassword=my_password`

The project can be build with this command:
`./gradlew clean build`

The project can be published with this command:
`./gradlew clean build publish`
