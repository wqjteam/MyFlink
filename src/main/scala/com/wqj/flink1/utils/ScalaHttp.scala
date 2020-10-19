package com.wqj.flink1.utils

import scalaj.http._

import scala.collection.mutable

object ScalaHttp {
  def get(url: String, param: mutable.HashMap[String, String]): HttpResponse[String] = {
    val response: HttpResponse[String] = Http("http://foo.com/search").param("q", "monkeys").asString
    response.body
    response.code
    response.headers
    response.cookies
    response
  }

  def post(url: String, param: mutable.HashMap[String, String]): HttpResponse[String] = {
    val consumer = Token("key", "secret")
    val response = Http("https://api.twitter.com/oauth/request_token").postForm(Seq("oauth_callback" -> "oob"))
      .oauth(consumer).asToken

    println("Go to https://api.twitter.com/oauth/authorize?oauth_token=" + response.body.key)

    val verifier = Console.readLine("Enter verifier: ").trim

    val accessToken = Http("https://api.twitter.com/oauth/access_token").postForm.oauth(consumer, response.body, verifier).asToken

    println(Http("https://api.twitter.com/1.1/account/settings.json").oauth(consumer, accessToken.body).asString)
    val response2 = Http("https://api.twitter.com/1.1/account/settings.json").oauth(consumer, accessToken.body).asString
    response2
  }
}
