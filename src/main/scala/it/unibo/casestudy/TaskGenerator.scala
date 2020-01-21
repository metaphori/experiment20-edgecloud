package it.unibo.casestudy

import it.unibo.alchemist.model.scafi.ScafiIncarnationForAlchemist._
import it.unibo.casestudy.Services._

class TaskGenerator extends AggregateProgram with StandardSensors with ScafiAlchemistSupport {

  lazy val providedServices: Set[ProvidedService] = {
    if(node.has("providedServices"))
      node.get[Set[ProvidedService]]("providedServices")
    else
      Set.empty
  }

  override def main(): Any = {
    branch(!node.has("task")) {
      val k = rep(0)(x => if(x==0) 1 else 2)

      if(k == 2) { // && timestamp() < 250) {
        val task = Task(
          (randomGenerator().shuffle(services) -- providedServices.map(_.service))
            .take(1 + randomGenerator().nextInt(3))
        )
        node.put("task", task)
        node.put("taskTime", timestamp())
        node.put("tasks", if (node.has("tasks")) node.get[Int]("tasks") + 1 else 1)
      }
    }{ }
  }
}