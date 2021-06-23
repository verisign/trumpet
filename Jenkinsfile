def sh_with_results(cmd) {
  sh "${cmd} > result.tmp"

  def out = readFile('result.tmp')
  def res = out.tokenize('\n')
  sh 'rm result.tmp'
  echo out
  res
}
node
{
    docker.withRegistry('https://docker.vrsn.com/')
    {
        docker.image('vscc/docker-full-build:1.0-mvn3.6.3-jdk8').inside()
        {
            stage('Preparation')
            {
                git branch: '${GIT_BRANCH}',
                credentialsId: 'cm-teamcity-github-credentials',
                url: 'git@github.vrsn.com:VSCC/trumpet.git'
            }
            stage('Build')
            {
                sh 'rm -rf /home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/'
                sh 'export PATH=$PATH:/bin'
                sh "mvn clean install -DskipTests -DrpmRelease=${BUILD_NUMBER} -Prpm -Pcdp714"
            }
            stage('Publish Jar Artifacts in Artifactory') 
            {
                version = sh_with_results('mvn -q -Dexec.executable=echo -Dexec.args=\'${project.version}\' --non-recursive exec:exec')[0]

                if ( GIT_BRANCH == 'master'){
                    targetLibs = "libs-release-local"
                }
                else {
                     targetLibs = "libs-snapshot-local"
                }

                //Push artifacts to artifactory
                rtUpload(
                    serverId: "vrsn-artifactory", 
                    failNoOp: true,
                    spec: """ {
                        "files": [
                        {
                        "pattern": "/home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/trumpet-server/*/trumpet-server*.*",
                        "target": "${targetLibs}/com/verisign/vscc/trumpet/trumpet-server/${version}/"
                        },
                        {
                        "pattern": "/home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/trumpet-common/*/trumpet-common*.*",
                        "target": "${targetLibs}/com/verisign/vscc/trumpet/trumpet-common/${version}/"
                        },
                        {
                        "pattern": "/home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/trumpet-client/*/trumpet-client*.*",
                        "target": "${targetLibs}/com/verisign/vscc/trumpet/trumpet-client/${version}/"
                        },
                        {
                        "pattern": "/home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/trumpet-examples/*/trumpet-examples*.*",
                        "target": "${targetLibs}/com/verisign/vscc/trumpet/trumpet-examples/${version}/"
                        },
                        {
                        "pattern": "/home/jenkins/workspace/Verisign/VSCC-ENG/VSCC-Trumpet/?/.m2/repository/com/verisign/vscc/trumpet/trumpet-parent/*/trumpet-parent*.*",
                        "target": "${targetLibs}/com/verisign/vscc/trumpet/trumpet-parent/${version}/"
                        }
                        ]
                    } """)
                rtPublishBuildInfo(
                    serverId: "vrsn-artifactory"
                )
            }
            stage('Publish RPM Artifacts in Artifactory') 
            {
                version = sh_with_results('mvn -q -Dexec.executable=echo -Dexec.args=\'${project.version}\' --non-recursive exec:exec')[0]
                rtUpload(
                    serverId: "vrsn-artifactory", 
                    failNoOp: true,
		    buildName: "VSCC/trumpet",
                    spec: """ {
                        "files": [
                        {
                        "pattern": "**/target/rpm/*/RPMS/*/*.rpm",
                        "target": "yum-dev-local/noarch/trumpet/trumpet-${version}/"
                        }
                        ]
                    } """
                    )
            }
            

            stage ('Archive artifacts')
            {
                archiveArtifacts artifacts: '**/target/*.jar', fingerprint: true    
                archiveArtifacts artifacts: '**/target/rpm/*/RPMS/*/*.rpm', fingerprint: true    
            }
        }
    }
}
