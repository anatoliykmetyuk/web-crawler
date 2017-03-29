package crawler

import java.net.URL

import scala.collection.JavaConverters._

import org.jsoup.nodes._
import org.jsoup._

import akka.actor._

import Main.fetch
import Protocol._

object ActorBased {
  def main(args: Array[String]): Unit = {
    // val system = ActorSystem("PiSystem")
    // val root   = system actorOf Worker.workerProps

    // (root ? Job(new URL("http://mvnrepository.com/"), 1)).onSuccess {
    //   case Result(res) =>
    //     println(res.take(10).mkString("\n"))
    //     println(res.size)

    // }


  }
}

class Worker extends Actor {
  var buffer  : Set[URL]      = Set()
  var children: Set[ActorRef] = Set()

  var answered = 0

  def receive = awaitingForTasks

  def dispatch(lnk: URL, depth: Int, visited: Set[URL]): Unit = {
    val child = context actorOf Worker.workerProps
    children += child
    child ! Job(lnk, depth, visited)
  }

  def awaitingForTasks: Receive = {
    case Job(url, depth, visited) =>
      val links = fetch(url).getOrElse(Set()).filter(!visited(_))
      
      buffer   = links
      children = Set()
      answered = 0

      for { l <- links } dispatch(l, depth - 1, visited)
      context become processing
  }

  def processing: Receive = {
    case Result(urls) =>
      buffer ++= urls
      if (answered == children.size) sender ! Result(buffer)
      context stop self
  }
}

object Worker {
  def workerProps: Props = Props(classOf[Worker])
}


object Protocol {
  sealed trait Msg
  case class Job(url : URL, depth: Int, visited: Set[URL] = Set()) extends Msg
  case class Result (urls: Set[URL]) extends Msg
}