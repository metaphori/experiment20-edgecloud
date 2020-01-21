package it.unibo.casestudy

import it.unibo.alchemist.model.scafi.ScafiIncarnationForAlchemist.ID

trait Service

case object Database extends Service

case object OCR extends Service

case object SpeechRecogniser extends Service

trait IProvidedService {
  val service: Service
  val numInstances: Int
  var allocatedInstances: Int
}

case class ProvidedService(service: Service)
                          (val numInstances: Int = 1, var allocatedInstances: Int = 0) extends IProvidedService {
  def freeInstances = numInstances - allocatedInstances

  override def toString: String = s"ProvidedService($service)(numInstances=$numInstances, allocatedInstances=$allocatedInstances)"
}

case class ProvisionedService(provisioner: ID, service: Service)

case class Task(requiredServices: Set[Service])

case class TaskRequest(requestor: ID, task: Task)
                      (val allocation: Map[Service,ID]){
  def missingServices: Set[Service] = task.requiredServices -- allocation.keySet

  override def toString: String = s"TaskRequest(requestor=$requestor, task=$task)(allocation=$allocation)"
}

object Services {
  val services: Set[Service] = Set(Database, OCR, SpeechRecogniser)
}