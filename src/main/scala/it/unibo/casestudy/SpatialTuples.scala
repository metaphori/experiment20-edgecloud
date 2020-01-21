package it.unibo.casestudy

import it.unibo.alchemist.model.implementations.positions.Euclidean2DPosition
import it.unibo.alchemist.model.scafi.ScafiIncarnationForAlchemist._
import it.unibo.scafi.space.{Point2D, Point3D}
import scala.collection.JavaConverters._

class SpatialTuples extends AggregateProgram with StandardSensors with Gradients with BlockG with ScafiAlchemistSupport
  with FieldCalculusSyntax with FieldUtils with CustomSpawn with BlockS with BlockC {
  val tupleProcessPrefix = "tp_"

  var k = 0
  var tuples: Set[String] = Set()
  var events: Set[ProcessEvent] = Set()

  override def currentPosition(): Point3D = {
    val pos = sense[Euclidean2DPosition](LSNS_POSITION)
    Point3D(pos.getX, pos.getY, 0)
  }

  def current2DPosition(): Point2D = Point2D(currentPosition().x, currentPosition().y)

  override def main(): Any = {
    tuples ++= tupleManagement()
    node.put("current_tuples", tuples)
    node.put("events", events)

    k = rep(0)(_+1)

    // From the beginning
    outInRegion("out-in-circle", RectangularRegion(Point2D(88,38), Point2D(110,62)), Some("out2"))

    branch(mid==1 && k==1) { out("hello", extension = 20+Math.random()*50, Some("out1")) }{}

    branch(mid==152){ outHere("out-in-152", 12, Some("out3")) }{}

    branch(mid==19 && k==20) { in("hello", Some("in1")) }{}

    branch(mid==152 && k==150){ read("hello",Some("read1")) }{}

    branch(mid==29 && k==170) { out("hello", extension = 20+Math.random()*50, Some("out1")) }{}
  }

  private var pendingTupleOps = Set[TupleOpId]()

  trait Region {
    def withinRegion(p: Point2D): Boolean
  }
  case class RectangularRegion(start: Point2D, end: Point2D) extends Region {
    override def withinRegion(p: Point2D): Boolean =
      p.x >= start.x && p.x <= end.x && p.y >= start.y && p.y <= end.y
  }
  case class CircularRegion(center: Point2D, radius: Double) extends Region {
    override def withinRegion(p: Point2D): Boolean = Math.sqrt(Math.pow(p.x-center.x, 2) + Math.pow(p.y-center.y, 2)) <= radius
  }

  type Tuple = String
  type TupleTemplate = String

  trait TupleOp
  case class OutMe(datum: Tuple, val initiator: ID, val extension: Double = 0) extends TupleOp
  case class OutHere(datum: Tuple, val extension: Double = 0) extends TupleOp
  case class OutInRegion(datum: Tuple, val region: Region) extends TupleOp
  case class Read(template: TupleTemplate, val initiator: ID, val extension: Double = Double.PositiveInfinity) extends TupleOp
  case class In(template: TupleTemplate, val initiator: ID, val extension: Double = Double.PositiveInfinity) extends TupleOp

  case class TupleOpId(uid: String)(val op: TupleOp)
  case class TupleResult(datum: Tuple)

  trait ProcessEvent
  case class TupleRemovalRequested(by: TupleOpId, tuple: Tuple) extends ProcessEvent
  case class TupleRemovalOk(by: TupleOpId) extends ProcessEvent
  case object TupleRemovalDone extends ProcessEvent

  import Spawn._

  def consensusOn[T](data: Option[T], leader: Boolean, potential: Double): Boolean = {
    val collect = C[Double,Set[T]](potential, _++_, data.toSet, Set.empty)
    val choice = rep[Option[T]](None){ v => v.orElse(collect.headOption) }
    broadcastUnbounded(leader && !choice.isEmpty, choice) == data && !data.isEmpty
  }

  def tupleOperation(toid: TupleOpId)(arg: ProcArg): (TupleResult, Status) = {
    node.put(toid.uid, 1)
    val res = toid.op match {
      case outop@OutMe(s, initiator, extension) => OutMeLogic(toid, outop, arg)
      case outop@OutInRegion(s, region) => OutInRegionLogic(toid, outop, arg)
      case readop@Read(ttemplate, initiator, extension) => ReadLogic(toid, readop, arg)
      case inop@In(ttemplate, initiator, extension) => InLogic(toid, inop, arg)
      case _ => (TupleResult("invalid"), Terminated)
    }

    node.put(toid.uid+"_status", if(res._2==Output) 2 else if(res._1==Bubble) 1 else 0)
    //println(s"[$mid] $toid -> $res")
    res
  }

  def OutMeLogic(toid: TupleOpId, outOp: OutMe, arg: ProcArg): (TupleResult, Status) = {
    val OutMe(s, initiator, extension) = outOp

    val g = classicGradient(initiator==mid)

    val terminate = handleRemovalByIN(toid, initiator, s)

    (TupleResult(s), if(terminate) Terminated else if(g<=extension) Output else External)
  }

  def handleRemovalByIN(toid: TupleOpId, initiator: ID, s: Tuple): Boolean = {
    val g = classicGradient(initiator==mid)

    val processWhoDidIN = events.collectFirst {
      case ev@TupleRemovalRequested(tid, `s`) => ev
    }.map(_.by)

    val gotConfirmation = consensusOn(data = processWhoDidIN, leader = initiator==mid, potential = g)

    processWhoDidIN.foreach(p => if(gotConfirmation) emitEvent(TupleRemovalOk(toid)))

    // Return whether the OUT process must terminate (i.e., we are done) or not
    events.collectFirst { case TupleRemovalDone => true }.getOrElse(false)
  }

  def OutInRegionLogic(toid: TupleOpId, outOp: OutInRegion, arg: ProcArg): (TupleResult, Status) = {
    val OutInRegion(s, region) = outOp
    val pos = currentPosition()
    (TupleResult(s), if(region.withinRegion(Point2D(pos.x, pos.y))) Output else External)
  }

  def ReadLogic(toid: TupleOpId, readOp: Read, arg: ProcArg): (TupleResult, Status) = {
    val ProcArg(localTuples, localProcs) = arg
    val Read(ttemplate, initiator, extension) = readOp

    val g = classicGradient(initiator==mid)
    val tupleFound = localTupleMatching(localTuples, ttemplate)
    val result  = broadcastUnbounded[Option[Tuple]](!tupleFound.isEmpty, tupleFound)
    val (gotIt,canClose) = rep((false,false))(f => {
      val shouldBeDone = mid==initiator && !result.isEmpty
      (shouldBeDone, f._2 || (shouldBeDone && f._1))
    })
    (TupleResult(result.getOrElse("not-found")), if(mid==initiator){
      if(canClose) Terminated else if(gotIt) Output else Bubble
    } else if(g<extension) Bubble else External)
  }

  def InLogic(toid: TupleOpId, inOp: In, arg: ProcArg): (TupleResult, Status) = {
    val ProcArg(localTuples, localProcs) = arg
    val In(ttemplate, initiator, extension) = inOp
    // Note: IN is not trivial:
    // - Need consensus on the tuple to remove
    // - As there might be multiple concurrent INs, these must be discriminated by a tuple's owner
    // So, it needs 2 feedback loops for 4 flows of events:
    // 1) TupleRemovalRequested(by,tuple)
    // 2) TupleRemovalOk(by)
    // 3) TupleRemovalDone
    // 4) TupleRemovalEnd
    val g = classicGradient(initiator==mid)

    val tupleFound = localTupleMatching(localTuples, ttemplate)
    val myTupleChosen = consensusOn(tupleFound, initiator==mid, g)

    if(myTupleChosen){
      // Once the device who found the tuple sees that its tuple is the chosen one, it must get confirmation by tuple's owner
      // Tuple removal must affect Out processes
      emitEvent(TupleRemovalRequested(toid, tupleFound.get))
    }

    val outProcess = events.collectFirst[TupleOpId]{ case TupleRemovalOk(by) => by }
    val gotAck = !outProcess.isEmpty
    val canRetrieveTheTuple = broadcastUnbounded(gotAck, gotAck)

    val result  = broadcastUnbounded[Option[String]](myTupleChosen && canRetrieveTheTuple, tupleFound)

    val didRead = broadcastUnbounded(mid==initiator, mid==initiator && !result.isEmpty)
    if(myTupleChosen && didRead){ emitEvent(TupleRemovalDone) }
    val done = broadcastUnbounded(myTupleChosen && didRead, (myTupleChosen && didRead) && localProcs.forall(_!=outProcess))

    // IN bubble closing similar to read
    val (gotIt,canClose) = rep((false,false))(f => {
      val shouldBeDone = mid==initiator && !result.isEmpty && done
      (shouldBeDone, f._2 || (shouldBeDone && f._1))
    })
    (TupleResult(result.getOrElse("not-found")), if(mid==initiator){
      if(canClose) Terminated else if(gotIt) Output else Bubble
    } else if(g<extension) Bubble else External)
  }

  def localTupleMatching(localTuples: Set[Tuple], t: TupleTemplate): Option[Tuple] =
    rep(None:Option[Tuple]) { v =>
      if(!v.isEmpty && localTuples.contains(v.get)) v else localTuples.collectFirst {
        case tuple if tuple.matches(t) => tuple
      }
    }

  def emitEvent(event: ProcessEvent): Unit = {
    events += event
  }

  def broadcastUnbounded[V](source: Boolean, field: V): V =
    G[V](source, field, v => v, nbrRange)

  case class ProcArg(localTuples: Set[Tuple], procs: Set[TupleOpId])

  def tupleManagement(): Set[String] = {
    // Visualize active process instances after cleaning old ones
    val thisNode = alchemistEnvironment.getNodeByID(mid)
    thisNode.getContents.asScala.foreach(tp => {
      if(tp._1.getName.startsWith(tupleProcessPrefix)) thisNode.removeConcentration(tp._1)
    })

    val tuples = rep(Map[TupleOpId, TupleResult]())(procs => {
      val arg = ProcArg(procs.values.map(_.datum).toSet, procs.keySet)
      sspawn[TupleOpId,ProcArg,TupleResult](tupleOperation, pendingTupleOps, arg)
    })

    node.put("zProcesses", tuples)

    // Clear structure for pending ops
    pendingTupleOps = Set()

    tuples.values.map(_.datum).toSet
  }

  def out(data: String, extension: Double, optId: Option[String]) = {
    pendingTupleOps += TupleOpId(tupleProcessPrefix+optId.getOrElse(s"out_${mid}_${k}"))(OutMe(data, mid, extension))
  }

  def outInRegion(data: String, region: Region, optId: Option[String]) = {
    pendingTupleOps += TupleOpId(tupleProcessPrefix+optId.getOrElse(s"out_${mid}_${k}"))(OutInRegion(data, region))
  }

  def outHere(data: String, radius: Double, optId: Option[String]) = {
    pendingTupleOps += TupleOpId(tupleProcessPrefix+optId.getOrElse(s"out_${mid}_${k}"))(OutInRegion(data, CircularRegion(current2DPosition(), radius)))
  }

  def read(data: String, optId: Option[String]) = {
    pendingTupleOps += TupleOpId(tupleProcessPrefix+optId.getOrElse(s"read_${mid}_${k}"))(Read(data, mid))
  }

  def in(data: String, optId: Option[String]) = {
    pendingTupleOps += TupleOpId(tupleProcessPrefix+optId.getOrElse(s"in_${mid}_${k}"))(In(data, mid))
  }

}