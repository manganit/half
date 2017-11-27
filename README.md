# half
Hadoop Light Application Framework


Releasing Half :

# Deploy to Sonatype snapshot repo

mvn clean deploy -P release

git push

# Release artifact version

mvn release:clean

mvn release:prepare

# Deploy to Sonatype staging repo

mvn release:perform

git push

# Finally, find the commanganitXXXX repo, 
# then close and release manually from the Sonatype OSSRH webui

