package wandou.avro

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import wandou.avro.test.Account

class FromToJsonSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val schema = Schemas.account

  "FromToJson" when {
    "FromtoJsonString" in {
      val account = new Account
      account.setId("1")
      account.setRegisterTime(System.currentTimeMillis())
      val jsonStr = ToJson.toJsonString(account, schema)
      assertResult(account.toString)(FromJson.fromJsonString(jsonStr, schema).toString)
    }
  }
}
