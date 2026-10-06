pipeline {
    agent any

    environment {
        OVERRIDE_SERVICE_NAME   = 'optimus-gateway'
        DEPLOY_HOST_DEV         = '10.200.4.23'
        DEPLOY_HOST_PROD        = ''
        DEPLOY_USER             = 'halim'
        APP_DIR                 = '/home/halim/optimus'
        GITHUB_COMMIT_ID        = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
        GITHUB_COMMIT_AUTHOR    = sh(script: "git log -1 --pretty=format:'%an'", returnStdout: true).trim()
        GITHUB_COMMIT_MESSAGE   = sh(script: "git log -1 --pretty=format:'%s'", returnStdout: true).trim()
    }

    stages {
        stage('Start') {
            steps {
                script {
                    def causes      = currentBuild.getBuildCauses()
                    def triggerInfo = getTriggerInfo(causes, GITHUB_COMMIT_ID, GITHUB_COMMIT_AUTHOR, GITHUB_COMMIT_MESSAGE, "Started by")
                    def msg         = """🚀 *STARTING*: Job '${env.JOB_NAME}' [${env.BUILD_NUMBER}] (${env.BUILD_URL})
    ----------------------------------------
    ${triggerInfo}"""
                    googlechatnotification url: "${GCHAT_WEBHOOK}", message: msg
                }
            }
        }

        stage('Test') {
            steps {
                sh 'go vet ./... && go test ./...'
            }
        }

        stage('Build') {
            steps {
                sh 'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/gateway cmd/gateway/main.go'
                archiveArtifacts artifacts: 'bin/gateway', fingerprint: true
            }
        }

        stage('Deploy Branch Development') {
            when { branch 'development' }
            steps {
                sh '''
                    set -e
                    SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=accept-new"
                    TARGET="${DEPLOY_USER}@${DEPLOY_HOST_DEV}"

                    scp $SSH_OPTS bin/gateway ${TARGET}:${APP_DIR}/gateway.new
                    ssh $SSH_OPTS ${TARGET} "APP_DIR=${APP_DIR} SERVICE=${OVERRIDE_SERVICE_NAME} COMMIT_ID=${GITHUB_COMMIT_ID} bash -s" < deploy/remote-deploy.sh
                '''
            }
        }

        stage('Deploy Branch Main') {
            when { branch 'main' }
            steps {
                sh '''
                    set -e
                    SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=accept-new"
                    TARGET="${DEPLOY_USER}@${DEPLOY_HOST_PROD}"

                    scp $SSH_OPTS bin/gateway ${TARGET}:${APP_DIR}/gateway.new
                    ssh $SSH_OPTS ${TARGET} "APP_DIR=${APP_DIR} SERVICE=${OVERRIDE_SERVICE_NAME} COMMIT_ID=${GITHUB_COMMIT_ID} bash -s" < deploy/remote-deploy.sh
                '''
            }
        }
    }

    post {
        success {
            script {
                def triggerInfo = getTriggerInfo(currentBuild.getBuildCauses(), GITHUB_COMMIT_ID, GITHUB_COMMIT_AUTHOR, GITHUB_COMMIT_MESSAGE, "Deployed by")
                sendNotification("✅ *SUCCESSFUL*", triggerInfo)
            }
        }

        unstable {
            script {
                def triggerInfo = getTriggerInfo(currentBuild.getBuildCauses(), GITHUB_COMMIT_ID, GITHUB_COMMIT_AUTHOR, GITHUB_COMMIT_MESSAGE, "Deployed by")
                sendNotification("⚠️ *UNSTABLE*", triggerInfo)
            }
        }

        failure {
            script {
                def triggerInfo = getTriggerInfo(currentBuild.getBuildCauses(), GITHUB_COMMIT_ID, GITHUB_COMMIT_AUTHOR, GITHUB_COMMIT_MESSAGE, "Deployed by")
                sendNotification("❌ *FAILED*", triggerInfo)
            }
        }

        aborted {
            script {
                def triggerInfo = getTriggerInfo(currentBuild.getBuildCauses(), GITHUB_COMMIT_ID, GITHUB_COMMIT_AUTHOR, GITHUB_COMMIT_MESSAGE, "Deployed by")
                sendNotification("⚪ *ABORTED*", triggerInfo)
            }
        }

        cleanup {
            cleanWs()
        }
    }
}


// Helper methods
def getTriggerInfo(causes, commitId, author, message, labelPrefix) {
    if (causes.find { it._class.contains('UserIdCause') }) {
        return  "👤 ${labelPrefix} ${causes.find { it._class.contains('UserIdCause') }.userId}\n" +
                "🆔 Commit ID: `${commitId}`\n" +
                "💬 Commit Message: ${message}"
    } else if (causes.find { it._class.contains('GitHubPushCause') }) {
        return  "🤖 ${labelPrefix} GitHub push by ${author}\n" +
                "🆔 Commit ID: `${commitId}`\n" +
                "💬 Commit Message: ${message}"
    } else if (causes.find { it._class.contains('TimerTriggerCause') }) {
        return  "⏰ ${labelPrefix} scheduled build\n" +
                "🆔 Commit ID: `${commitId}`"
    }
    return "❓ ${labelPrefix} unknown"
}

def getGitAuthor() {
    return sh(script: "git log -1 --pretty=format:'%an'", returnStdout: true).trim()
}

def getGitMessage() {
    return sh(script: "git log -1 --pretty=format:'%s'", returnStdout: true).trim()
}

def sendNotification(status, triggerInfo) {
    googlechatnotification url: "${GCHAT_WEBHOOK}",
        message: """${status}: Job '${env.JOB_NAME}' [${env.BUILD_NUMBER}] (${env.BUILD_URL})
----------------------------------------
${triggerInfo}
⏱️ Build Duration: ${currentBuild.durationString}"""
}