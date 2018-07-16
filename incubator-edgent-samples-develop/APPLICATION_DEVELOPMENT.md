<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Edgent Application Development, Packaging and Execution.

To develop Edgent applications you will utilize the 
Edgent SDK/runtime jars and package your application
artifacts for deploying to an edge device or gateway for execution.

The Edgent SDK/runtime jars are published to the 
[ASF Nexus Releases Repository](https://repository.apache.org/content/repositories/releases/)
and the Maven Central Repository.
Alternatively, you can build the Edgent SDK yourself from a source release
and the resulting jars will be added to your local maven repository.
  
There are a set of Edgent jars for each supported platform: java8, java7, and android.
The maven artifact groupIds for the Edgent jars are:

- `org.apache.edgent`  - for java8,
- `org.apache.edgent.java7`
- `org.apache.edgent.android`

Note, the Java package names for Edgent components do not incorporate
the platform kind; the package names are the same regardless of the platform.

See the release's `JAVA_SUPPORT` information in [downloads](https://http://edgent.incubator.apache.org/docs/downloads)
for more information on artifact coordinates, etc.


## Writing Your Application

The Edgent API is most easily used by using Java8 lambda expressions.
If you only want to deploy your Edgent application to a java8 environment
then your application may use any java8 features it chooses.  You compile
and run against the Edgent java8 jars.

If you want to deploy your Edgent application to a java7 or android
environment, it's still easiest to write your application using the Edgent APIs
with java8 lambda expressions.  You compile with java8 but constrain 
your application to using java7 features plus java8 lambda expressions.
The Retrolambda tool is used to convert your application's generated 
class files to java7.
The Edgent java7 and android platform jars were created in that manner too.
Your application would then be run against the appropriate
Edgent platform jars. 

Alternatively you can forgo the use of lambda
expressions and write your application in java7 and compile
and run against the appropriate Edgent platform jars.

For convenience it's easiest to build your Edgent application using 
maven-repository-enabled build tooling (e.g., maven, maven-enabled
Eclipse or IntelliJ).  The tooling transparently downloads the 
required Edgent jars from the maven repository if they aren't
already present in your local maven repository.

### Edgent Application Template
 
You can clone the `template` project as a starting point for your
Edgent application. See [samples/template/README.md](template/README.md).

TODO: we would like to provide a maven Edgent Application archetype
that users can use to create an application project template.

### Using Non-maven-integrated Tooling

If you can't or don't want to use maven-repository-enabled tooling
you will need to get a local copy of the Edgent jars and their
dependencies and add them to your compile classpath.  This case
is covered in subsequent sections.

## Packaging and Execution

Edgent doesn't provide any "deployment" mechanisms other than its primitive
"register jar" feature (see the `IotProvider` javadoc).  Generally, managing
the deployment of application and Edgent jars to edge devices is left to 
others (as an example, the IBM Watson IoT Platform has device APIs to
support "firmware" download/update).

To deploy an Edgent application to a device like a Raspberry Pi, 
you could just FTP the application to the device and modify the
device to start the application upon startup or on command.
Also see the `cron` folder in the Edgent samples.

To run your Edgent application on an edge device, your application
jar(s) need to be on the device.  Additionally, the application's 
dependent Edgent jars (and their transitive dependencies) need to
be on the device.  It's unlikely the device will be able to retrieve
the dependencies directly from a remote maven repository such as
maven central.

Here are three options for dealing with this.

### Option 1: Create an uber-jar for your application

The uber jar is a standalone entity containing
everything that's needed to run your application.

The uber jar contains the application's classes and
the application's dependent Edgent classes and their
transitive dependencies.

The template project's pom and
the Edgent samples poms contain configuration information
that generates an uber jar in addition to the standard
application jar.  Eclipse can also export an uber jar.

You run your application like:
    `java -cp <path-to-uber-jar> <full-classname-of-main-class>`

### Option 2: Separately manage the application and Edgent jars

Copy the application's jars to the device.
Get a copy of the Edgent jars and their dependencies
onto the device. It's possible for multiple Edgent
applications to share the Edgent jars.

The Apache Edgent project does not release a
binary bundle containing all of the Edgent jars
and their dependencies.  The binary artifacts
are only released to maven central.

See [samples/get-edgent-jars-project](get-edgent-jars-project/README.md)
for a tool to get a copy of the Edgent jars.

### Option 3: Create an application package bundle

The bundle is a standalone entity containing
everything that's needed to run your application.
   
The bundle is copied to the device and unpacked.
A run script forms the appropriate `CLASSPATH`
to the package's jars and starts the application.

The `package-app.sh` script included with the
Edgent samples creates an application bundle.

The application bundle contains the application's jar,
the application's dependent Edgent jars (as specified in
the application's pom) and the Edgent jars' dependencies,
and a run-app.sh script.

The application's dependent Edgent runtime jars and 
their dependencies are retrieved from a local or remote
maven repository.

If the application's execution environment is
java7 or android, use the appropriate script options
to retrieve the appropriate Edgent platform jars for
execution.

The generated run-app.sh script configures the CLASSPATH
and runs the application.

E.g.,

``` sh
cd MyApp # the application's project directory
package-app.sh --mainClass com.mycompany.app.MyApp --appjar target/my-app-1.0-SNAPSHOT.jar
##### get the app specific dependencies...
...
##### create target/app-run.sh...
##### create target/app-pkg.tar...
##### Copy target/app-pkg.tar to the destination system"
##### To run the app:"
#####     mkdir app-pkg"
#####     tar xf app-pkg.tar -C app-pkg"
#####     (cd app-pkg; ./app-run.sh)"
```

For more usage information:

``` sh
./package-app.sh -h
```
