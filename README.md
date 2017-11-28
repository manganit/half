# Half - Hadoop Light Application Framework

### What is Half ? :
Half is basically a Java library that helps you to write applications that interact with Hadoop components.
Basically, Half takes care of :
    - Hadoop Configuration management from config files, Oozie context and runtime arguments
    - Kerberos authentication from a keytab file, a ticket cache or a token given by Oozie
    - Debug from your IDE, run as an Hadoop Tool, or deploy as an Oozie Java action with no code adaptation
    - Client libs and dependencies management following main Hadoop distributions (currently CDH and HDP)

### How to start :
Have a look at https://github.com/manganit/half-quickstart is the shortest way to learn Half

### How to contribute :
Feel free to fork, make pull requests and a member the Manganit.com team will review your contribution.

### How to release :

- Deploy to Sonatype snapshot repo
```sh
$ mvn clean deploy -P release
$ git push
```
- Release artifact version
```sh
mvn release:clean
mvn release:prepare
```
- Deploy to Sonatype staging repo
```sh
mvn release:perform
git push
```
- Finally, find the commanganitXXXX repo, 
- then close and release manually from the Sonatype OSSRH webui