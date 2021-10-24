pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
            }
        }
                
        stage("Build and start test image") {
            steps {
                sh "docker-composer build"
                sh "docker-compose up -d"
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
    post {
      always {
          sh "docker-compose down || true"
      }

      success {
          bitbucketStatusNotify buildState: "SUCCESSFUL"
      }

      failure {
          bitbucketStatusNotify buildState: "FAILED"
      }
    }
}