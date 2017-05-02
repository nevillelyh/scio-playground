import java.io._
import java.util.UUID
import java.util.function.{Function => JFunction}

import com.google.common.base.Charsets
import com.google.common.collect.{Maps, Queues}
import com.google.common.io.Files
import com.spotify.scio._
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.http4s.client.blaze.PooledHttp1Client

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object ScioJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val f = sc.parallelize(Seq("hello", "world", "scio", "post", "hadoop", "processing"))
      .applyTransform(ParDo.of(new HttpDoFn(100)))
      .applyTransform(ParDo.of(new PyDoFn(
        """
          |import sys
          |for l in sys.stdin:
          |    print l.rstrip().upper()
        """.stripMargin)))
      .materialize
    sc.close()
    f.waitForResult().value.foreach(println)
  }
}

class HttpDoFn(val maxTotalConnections: Int) extends DoFn[String, String] {
  private val uuid = UUID.randomUUID()
  private val futures = Maps.newConcurrentMap[UUID, Future[String]]
  private val results = Queues.newConcurrentLinkedQueue[String]()
  private val errors = Queues.newConcurrentLinkedQueue[Throwable]()

  // execution context that uses current thread
  @transient
  private lazy implicit val immediateExecutionContext = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = runnable.run()
    override def reportFailure(cause: Throwable): Unit = ExecutionContext.defaultReporter(cause)
  }

  @Setup
  def setup(): Unit = {
    ResourceManager.get("setup-" + uuid, () => {
      import sys.process._
      "apt-get update" #&& "apt-get install -y apache2 php5" #&& "/etc/init.d/apache2 start" !!

      Files.write(
        """<?php echo $_GET["input"] . "!!!"; ?>""",
        new File("/var/www/html/process.php"),
        Charsets.UTF_8)
    })
  }

  @StartBundle
  def startBundle(c: DoFn[String, String]#Context): Unit = {
    futures.clear()
    results.clear()
    errors.clear()
  }

  @ProcessElement
  def processElement(c: DoFn[String, String]#ProcessContext): Unit = {
    val client = ResourceManager.get("http-" + uuid, () => PooledHttp1Client(maxTotalConnections))
    val input = c.element()
    val url = "http://localhost/process.php?input=" + input

    import Futures._
    val fid = UUID.randomUUID()
    val future = client.expect[String](url).async
    future.onComplete {
      case Success(r) =>
        results.add(r)
        futures.remove(fid)
      case Failure(e) =>
        errors.add(e)
        futures.remove(fid)
    }
    futures.put(fid, future)
    flush(c)
  }

  @FinishBundle
  def finishBundle(c: DoFn[String, String]#Context): Unit = {
    Await.ready(Future.sequence(futures.values().asScala), Duration.Inf)
    flush(c)
  }

  private def flush(c: DoFn[String, String]#Context): Unit = {
    if (!errors.isEmpty) {
      val e = new RuntimeException("Failed to process futures")
      errors.asScala.foreach(e.addSuppressed)
      throw e
    }
    var r = results.poll()
    while (r != null) {
      c.output(r)
      r = results.poll()
    }
  }
}

class PyDoFn(code: String) extends DoFn[String, String] {
  private val uuid = UUID.randomUUID()
  private var file: File = _
  private var writer: PrintWriter = _
  private var future: Future[Unit] = _

  @Setup
  def setup(): Unit = {
    ResourceManager.get("setup-" + uuid, () => {
      import sys.process._
      "apt-get update" #&& "apt-get install -y python" !!
    })
    file = new File(sys.props("java.io.tmpdir"), s"$uuid.py")
    Files.write(code, file, Charsets.UTF_8)
  }

  @StartBundle
  def startBundle(c: DoFn[String, String]#Context): Unit = {
    val pos = new PipedOutputStream()
    val pis = new PipedInputStream(pos)
    writer = new PrintWriter(pos, true)

    import scala.concurrent.ExecutionContext.Implicits.global
    future = Future {
      import sys.process._
      (s"python $file" #< pis lineStream).foreach(c.output)
    }
  }

  @ProcessElement
  def processElement(c: DoFn[String, String]#ProcessContext): Unit = {
    writer.println(c.element())
  }

  @FinishBundle
  def finishBundle(c: DoFn[String, String]#Context): Unit = {
    writer.close()
    Await.ready(future, Duration.Inf)
  }

  @Teardown
  def teardown(): Unit = {
    file.delete()
  }
}

object ResourceManager {
  private val resources = Maps.newConcurrentMap[String, Any]()
  def get[T](key: String, f: () => T): T =
    resources.computeIfAbsent(key, new JFunction[String, T] {
      override def apply(t: String): T = f()
    }).asInstanceOf[T]
}

object Futures {
  import scalaz.concurrent.Task
  import scalaz.{\/-, -\/}

  // convert scalaz Task to Scala Future
  implicit class RichTask[A](val task: Task[A]) extends AnyVal {
    def async: Future[A] = {
      val p = Promise[A]()
      task.runAsync {
        case -\/(e) => p.failure(e)
        case \/-(r) => p.success(r)
      }
      p.future
    }
  }
}
