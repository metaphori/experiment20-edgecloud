package it.unibo.casestudy

import it.unibo.alchemist.model.implementations.positions.Euclidean2DPosition
import it.unibo.alchemist.model.scafi.ScafiIncarnationForAlchemist._
import it.unibo.casestudy.Services._
import it.unibo.scafi.space.{Point2D, Point3D}

class ServiceDiscovery extends AggregateProgram with StandardSensors with Gradients with BlockG with ScafiAlchemistSupport
  with FieldCalculusSyntax with FieldUtils with CustomSpawn with BlockS with BlockC {

  import Spawn._

  override def spawn[A, B, C](process: A => B => (C, Boolean), params: Set[A], args: B): Map[A,C] = {
    share(Map[A, C]()) { case (_, nbrProcesses) => {
      // 1. Take active process instances from my neighbours
      val nbrProcs = includingSelf.unionHoodSet(nbrProcesses().keySet)

      // 2. New processes to be spawn, based on a generation condition
      val newProcs = params

      // 3. Collect all process instances to be executed, execute them and update their state
      (nbrProcs.view ++ newProcs)
        .map { case arg =>
          val p = ProcInstance(arg)(a => { process(a) })
          vm.newExportStack
          val result = p.run(args)
          if(result.value.get._2) vm.mergeExport else vm.discardExport
          arg -> result
        }.collect { case(p,pi) if pi.value.get._2 => p -> pi.value.get._1 }.toMap
    } }
  }

  // Randomly initialises the set of services provided by a node
  lazy val providedServices: Set[ProvidedService] = {
    if(alchemistRandomGen.nextDouble() < node.get[Double]("hasServiceProbability"))
    randomGenerator()
      .shuffle(services)
      .take(randomGenerator().nextInt(services.size))
      .map(s => ProvidedService(s)(numInstances = 1)
    )
    else Set.empty
  }

  // Set of services currently allocated for a task (these should be checked before being advertised)
  var allocatedServices: Set[ProvidedService] = Set.empty

  // Same as the previous property, but used only in the spawn-based algorithm
  def offeredServices: Map[ProvidedService,TaskRequest] = if(node.has("offeredServices")){
    node.get[Map[ProvidedService,TaskRequest]]("offeredServices")
  } else {
    node.put("offeredServices", Map.empty)
    Map.empty
  }

  def addOfferedService(tr: TaskRequest, ns: ProvidedService) = {
    node.put("offeredServices", offeredServices + (ns -> tr))
  }

  def removeOfferedService(ns: Service) = {
    providedService(ns).foreach(ps => node.put("offeredServices", offeredServices - ps))
  }

  def providedService(ns: Service): Option[ProvidedService] =
    providedServices.find(_.service == ns)

  // Available services are those that are not already offered
  def availableServices: Set[ProvidedService] =
    providedServices.filter(_.freeInstances>0) -- offeredServices.keys

  // Holds a task that the node must accomplish by asking services to other devices
  def task: Option[Task] =
    if(node.has("task"))
      Some(node.get[Task]("task"))
    else
      None

  // Main program
  override def main(): Any = {
    node.put("providedServices", providedServices)
    node.put("hasTask", task.isDefined)
    node.put("t_time", currentTime())
    node.put("t_timestamp", timestamp())

    if(node.get[Boolean]("algorithm")){
      // Algorithm 1: uses a gradient to propagate request and collect services
      processBasedServiceDiscovery()
    } else {
      // Algorithm 2: same as Algorithm 1 but uses aggregate processes (spawn) to create overlapping service discovery bubbles
      classicServiceDiscovery()
    }
  }

  def classicServiceDiscovery() = {
    val hasTask = task.isDefined
    val g = classicGradient(hasTask)
    val gHops = G[Int](hasTask, 0, _+1, nbrRange _)
    node.put("gradient", f"$g%2.1f")

    var taskChanged = false

    var mapOffers: Map[ID, (Service,Int)] = Map.empty

    // Task request includes services needed to accomplish a task + current allocations (services + providers)
    // Notice that variable 'task' is non-empty only in the initiator (i.e., the device which must accomplish a task)
    val taskRequest = rep[Option[TaskRequest]](None) { tr =>
        // Checks whether the task to be accomplished is different
        val theTask = task.map(t => TaskRequest(mid, t)(allocation = Map.empty))
        taskChanged = theTask != tr
        val localTaskRequest = if(tr==theTask) tr else theTask

        // The task executor broadcast the task request
        val receivedRequest = bcast(hasTask, localTaskRequest)
        node.put("receivedRequest", receivedRequest.isDefined)
        node.put("request", receivedRequest)

        if(receivedRequest.isEmpty && node.has("requestBy")) node.remove("requestBy")
        else if(receivedRequest.isDefined) node.put("requestBy", receivedRequest.get.requestor%20)

        // A node receiving a task request can offer "missing" services that are locally available
        val offeredServs: Set[Service] = receivedRequest.map { request =>
          // keep current allocation (I continue to offer services that I already offer)
          val currentlyOffered = request.allocation.filter(_._2 == mid).keySet
          // more allocations?
          val newOffers = request.missingServices.filter(ms => {
            val newOfferedService = availableServices.find(as => as.service == ms)
            newOfferedService.foreach(s => addOfferedService(request, s))
            newOfferedService.isDefined
          })
          // Make again available an offered service if the requestor has chosen the service from another device
          for(alloc <- request.allocation if alloc._2!=mid) {
              removeOfferedService(alloc._1)
          }
          // Make again available an offered service if the requestor has changed
          for(os <- offeredServices if os._2.requestor != request.requestor) {
            removeOfferedService(os._1.service)
          }
          currentlyOffered ++ newOffers
        }.getOrElse(Set.empty)
        node.put("offersService", offeredServs.nonEmpty)
        node.put("offeredService", offeredServs)

        // Collect offers and hops to requestor
        val offers = C[Double, Map[ID, (Service,Int)]](g, _ ++ _, offeredServs.map(s => mid -> (s,gHops)).toMap, Map.empty)
        mapOffers = offers
        // Update allocations based on offers
        localTaskRequest.map(t => t.copy()(allocation = chooseFromOffers(t, offers)))
    }

    // Time a device is trying to satisfy a task
    val tryFor: Long = branch(!taskChanged && taskRequest.exists(_.missingServices.nonEmpty) ){
      val start = rep(timestamp())(x => x)
      timestamp() - start
    }{ 0 }
    // Time a device has a task satisfied
    val keepFor: Long = branch(taskRequest.isDefined && taskRequest.get.missingServices.isEmpty){
      val start = rep(timestamp())(x => x)
      timestamp() - start
    }{ 0 }
    // Condition for task removal (accomplishment)
    val accomplished = keepFor > node.get[Number]("taskPropagationTime").longValue
    val giveUp = tryFor > node.get[Number]("taskConclusionTime").longValue
    // If the task is accomplished or the requestor gives up (certain services cannot be looked up), remove the task and record data
    if((accomplished || giveUp) && node.has("task")){
      node.remove("task")
      val latency: Int = (timestamp()-node.get[Long]("taskTime")).toInt
      node.put("taskLatency", if(node.has("taskLatency")) node.get[Number]("taskLatency").intValue()+latency else latency)
      val numHops: Int = taskRequest.get.allocation.values.map(mapOffers(_)._2).sum
      val cloudHops: Int = taskRequest.get.missingServices.map(_ => node.get[Number]("cloudcost").intValue()).sum
      val totalHops = numHops + cloudHops
      node.put("taskHops", if(node.has("taskHops")) node.get[Number]("taskHops").intValue() + totalHops else totalHops)
      if(accomplished) {
        node.put("completedTasks", if(node.has("completedTasks")) node.get[Number]("completedTasks").intValue() +1 else 1)
      }
      if(giveUp) {
        node.put("giveupTasks", if(node.has("giveupTasks")) node.get[Number]("giveupTasks").intValue()+1 else 1)
      }
    }
  }

  def chooseFromOffers(req: TaskRequest, offers: Map[ID,(Service,Int)]): Map[Service,ID] = {
    var servicesToAlloc = req.missingServices
    var newAllocations = Map[Service,ID]()
    for(offer <- offers.toList.sortBy(_._1)){
      if(servicesToAlloc.contains(offer._2._1)){
        newAllocations += offer._2._1 -> offer._1
        servicesToAlloc -= offer._2._1
      }
    }

    // keep current allocations if still provided, and add new allocations
    req.allocation.filter(curr => offers.contains(curr._2)) ++ newAllocations
  }

  def processBasedServiceDiscovery() = {
    val theTask = task.map(t => TaskRequest(mid, t)(allocation = Map.empty)).toSet

    val procs = sspawn[TaskRequest, Unit, TaskRequest](serviceDiscoveryProcess, theTask, ())
    node.put("procs", procs)
    node.put("numProcs", procs.size)
    node.put("numProcsOthers", procs.count(_._2.requestor!=mid))
    node.put("numProcsMine", procs.count(_._2.requestor==mid))

    for(os <- offeredServices if !procs.contains(os._2)){
      removeOfferedService(os._1.service)
    }

    procs.filter(_._2.requestor==mid)
  }

  def serviceDiscoveryProcess(taskRequest: TaskRequest)(args: Unit): (TaskRequest, Status) = {
    // Build a gradient from the souce (which is the requestor of the task and the initiator of the process)
    val source = taskRequest.requestor==mid
    val gHops = hopGradient(source)

    // Boolean variable indicating whether a device should quit the bubble or not
    var continueExpansion = true
    var mapOffers: Map[ID, (Service,Int)] = Map.empty

    val pid = s"proc_${taskRequest.hashCode()}_"

    node.put(pid+"hops", gHops)

    case class State(currentDistance: Int = 0, taskRequest: TaskRequest = taskRequest)
    val s = rep(State()){ oldState =>
      // Broadcast state from the source
      val receivedRequest = bcast(source, oldState)

      // The bubble must expand up to the num of hops indicated by the source
      continueExpansion = receivedRequest.currentDistance >= gHops-1 // && !receivedRequest.taskRequest.missingServices.isEmpty

      node.put(pid+"receivedRequest", receivedRequest)
      node.put(pid+"request", receivedRequest)
      node.put(pid+"requestBy", receivedRequest.taskRequest.requestor%20)

      // Determine the services which can be offered
      val servicesToOffer: Set[Service] =
        (offeredServices.filter(_._2 == receivedRequest.taskRequest).keys ++ receivedRequest.taskRequest.missingServices.flatMap(s => {
          val newServiceToOffer = availableServices.find(_.service == s).toSet
          newServiceToOffer.foreach(ns => addOfferedService(receivedRequest.taskRequest, ns))
          newServiceToOffer
        })).map(_.service).toSet

      node.put("numOfferedServices", offeredServices.size)

      // Collect offers and hops to the requestor
      val offers = C[Double, Map[ID, (Service,Int)]](gHops, _ ++ _, servicesToOffer.map(s => mid -> (s,gHops)).toMap, Map.empty)
      mapOffers = offers
      // Collect the bubble diameter to the requestor
      val maxExt = C[Double,Int](gHops, Math.max, gHops, -1)
      // Update allocation based on service offers
      val newTaskRequest = receivedRequest.taskRequest.copy()(allocation = chooseFromOffers(receivedRequest.taskRequest, offers))

      // Ask for expansion only if there are still some services to be provided
      State(if(newTaskRequest.missingServices.isEmpty) maxExt-1 else maxExt, newTaskRequest)
    }

    node.put(pid+"state", s)

    // Time a device is trying to satisfy a task
    val tryFor: Long = branch(s.taskRequest.missingServices.nonEmpty){
      val start = rep(timestamp())(x => x)
      timestamp() - start
    }{ 0 }
    // Time a device has a task satisfied
    val keepFor: Long = branch(s.taskRequest.missingServices.isEmpty){
      val start = rep(timestamp())(x => x)
      timestamp() - start
    }{ 0 }
    // Condition for task removal (accomplishment)
    val accomplished = keepFor > node.get[Number]("taskPropagationTime").longValue
    val giveUp = tryFor > node.get[Number]("taskConclusionTime").longValue
    val done = accomplished || giveUp
    if(done && node.has("task") && node.get[Task]("task")==s.taskRequest.task){
      node.remove("task")
      val latency: Int = (timestamp()-node.get[Long]("taskTime")).toInt
      node.put("taskLatency", if(node.has("taskLatency")) node.get[Number]("taskLatency").intValue()+latency else latency)
      val numHops: Int = s.taskRequest.allocation.values.map(mapOffers(_)._2).sum
      val cloudHops: Int = s.taskRequest.missingServices.map(_ => node.get[Number]("cloudcost").intValue()).sum
      val totalHops = numHops + cloudHops
      node.put("taskHops", if(node.has("taskHops")) node.get[Number]("taskHops").intValue() + totalHops else totalHops)
      if(accomplished) {
        node.put("completedTasks", if(node.has("completedTasks")) node.get[Number]("completedTasks").intValue() +1 else 1)
      }
      if(giveUp) {
        node.put("giveupTasks", if(node.has("giveupTasks")) node.get[Number]("giveupTasks").intValue() +1 else 1)
      }
    }

    val res = s match {
      case s if source && done => (s.taskRequest, Terminated)
      case s if source => (s.taskRequest, Output)
      case s => (s.taskRequest, if(continueExpansion) Output else External)
    }

    node.put(pid+"output", res)

    res
  }

  // Other stuff

  def hopGradient(src: Boolean): Int =
    classicGradient(src, () => 1).toInt

  def bcast[V](source: Boolean, field: V): V =
    G[V](source, field, v => v, nbrRange _)

  override def currentPosition(): Point3D = {
    val pos = sense[Euclidean2DPosition](LSNS_POSITION)
    Point3D(pos.getX, pos.getY, 0)
  }

  def current2DPosition(): Point2D = Point2D(currentPosition().x, currentPosition().y)
}