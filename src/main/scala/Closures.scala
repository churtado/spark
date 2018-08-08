object Closures {

  class Foo{
    def exec(f:(String) => Unit, name: String): Unit ={
      f(name)
   }
  }

  def main(args:Array[String]): Unit = {
    var hello = "Hello"
    def sayHello(name: String) {println(s"$hello, $name")}

    // a closure will be created around the hello variable

    val foo = new Foo
    foo.exec(sayHello, "Al")

    hello = "Hola"
    foo.exec(sayHello, "Lorenzo")
  }
}



