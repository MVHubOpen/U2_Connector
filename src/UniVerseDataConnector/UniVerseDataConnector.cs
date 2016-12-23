using System;
using U2.Data.Client;
using U2.Data.Client.UO;

namespace mvHub
{
    public class UniVerseDataConnector : ImvDataConnector
    {
        public override MvSession GetSession(string subroutine = null, string usernameOverride = null, string passwordOverride = null)
        {
            var session = new UniVerseSession(this, subroutine, usernameOverride, passwordOverride);
            return session;
        }
    }

    public class UniVerseSession : MvSession
    {
        public UniVerseSession(ImvDataConnector assignParentConnector,
            string subroutine,
            string usernameOverride,
            string passwordOverride) :
            base(assignParentConnector, subroutine, usernameOverride, passwordOverride)
        {


        }


        public override bool Call()
        {

            var con = new U2Connection
            {
                ConnectionString = new U2ConnectionStringBuilder
                {
                    UserID = User,
                    Password = Password,
                    Server = Hostname,
                    Database = Account,
                    AccessMode = "Native",
                    SessionIDAsIPAddress = true,
                    RpcServiceType = "uvcs",
                    ServerType = "UniVerse",
                    Pooling = false,
                    Connect_Timeout = 600
                }.ToString()
            };
            con.Open();

            var dbSession = con.UniSession;
            dbSession.BlockingStrategy = UniObjectsTokens.UVT_WAIT_LOCKED;
            dbSession.LockStrategy = UniObjectsTokens.UVT_EXCLUSIVE_READ;
            dbSession.ReleaseStrategy = UniObjectsTokens.UVT_READ_RELEASE;
            dbSession.Timeout = 6000;

            var dbSub = dbSession.CreateUniSubroutine(Subroutine+".HANDLER.SUB",4);
            dbSub.SetArg(0, RequestHeader);
            dbSub.SetArg(1, RequestBody);
            try
            {
                dbSub.Call();

            }
            catch (UniSubroutineException uEx)
            {
                throw new MvHubSubroutineException("Error Call Subroutine", uEx);

            }
            catch (Exception ex)
            {

                throw new MvHubSubroutineException("Error Call Subroutine", ex);
            }
            finally
            {
                ReplyHeader = dbSub.GetArg(2);
                ReplyBody = dbSub.GetArg(3);
                con.Close();

            }
            return true;

        }

        public override void Close()
        {
        }

        public override void Dispose()
        {
        }

    }
}
