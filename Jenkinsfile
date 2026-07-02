// agent_scheduler CI — Phase 01 harness (block_management.md §13).
// Runs the pytest suite. Extended in Phase 03 (Schedule + Bindings reshape).
pipeline {
  agent any
  options { timestamps() }
  stages {
    stage('pytest') {
      steps {
        sh 'python3 -m pytest -q tests/'
      }
    }
  }
  post {
    always { echo "agent_scheduler pipeline done" }
  }
}
