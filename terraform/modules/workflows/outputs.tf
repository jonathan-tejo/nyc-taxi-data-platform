output "workflow_name" {
  value = google_workflows_workflow.main_pipeline.name
}

output "workflow_id" {
  value = google_workflows_workflow.main_pipeline.id
}
