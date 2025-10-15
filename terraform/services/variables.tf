variable "services" {
  description = "A map of all the services to be deployed. The key is the service name and the value is an object with its specific configuration."
  type = map(object({
    image_name = string
    memory     = string
  }))
  default = {}
}