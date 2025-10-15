variable "name" { type = string }
variable "project_id" { type = string }
variable "location" { type = string }
variable "service_account_email" { type = string }
variable "container_image" { type = string }
variable "cpu" { 
    type = string
    default = "1" 
}
variable "memory" { 
    type = string
    default = "512Mi" 
}