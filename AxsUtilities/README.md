
To build with mvn:

```
docker run -it --rm  -v `pwd`:/usr/src/mymaven -v maven-repo:/root/.m2 -w /usr/src/mymaven maven:3-jdk-8 mvn package
```
