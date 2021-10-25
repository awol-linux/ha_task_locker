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
                sh "docker-compose up -d --build"
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
    }
}