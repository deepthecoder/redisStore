const _ = require("underscore");
    var AWS = require("aws-sdk");
    var redis = require("async-redis");
    
    const { awsfun } = require("../helpers/s3Helpres");
    
    var client = redis.createClient();
    
    const commentary = async (data, db) => {
     try {
     var innings = ["Zero", "First", "Second", "Third", "Fourth"];
     // const Commentary = db.collection('commentary')
     const CommentaryRevamp = db.collection("commentaryRevamp");
     // const CommentaryDump = db.collection('commentaryDump')
     const Matches = db.collection("matches");
     let findQuery = { matchfile: data.GameCode };
     let commentaryKey = "Commentary";
     var ifLive = true;
     if (typeof data.Match_Id != "undefined") {
     findQuery = { matchID: data.Match_Id };
     commentaryKey = "BallbyBallDetails";
     ifLive = false;
     }
     var cdcMatchData = await Matches.findOne(findQuery);
     if (cdcMatchData) {
     var players = _.union(cdcMatchData.homeSquad, cdcMatchData.awaySquad);
     var playerIdsWithName = {};
     var playersmap = players.map(x => {
     playerIdsWithName[x.playerID] = x.playerName;
     return x;
     });
     // console.log(data);
     var inning = data.InningNo;
     var details = {
     wd: "wide",
     b: "bye",
     W: "wicket",
     lb: "leg bye",
     nb: "no ball"
     };
     var wickets_ty = { b: "bowled", c: "caught", lbw: "lbw", st: "stumped" };
     var inningCommentary = _.filter(data[commentaryKey], z => {
     return z.Isball;
     }).map((x, i, array) => {
     let ballData = {};
    
     if (x.Isball) {
     ballData["ballIDCustom"] = (array ? array.length : 0) - i;
     ballData["matchID"] = cdcMatchData.matchID;
     ballData["ballID"] = x.Id;
     ballData["ballNumber"] = x.Over.split(".")[1];
     ballData["innings"] = inning;
     ballData["over"] = x.Over;
     ballData["overNumber"] = (
     Number(x.Over.split(".")[0]) + 1
     ).toString();
     ballData["commentary"] = x.Commentary;
     ballData["teamID"] =
     inning === "1"
     ? cdcMatchData["firstInningsTeamID"]
     : inning === "2"
     ? cdcMatchData["secondInningsTeamID"]
     : inning === "3"
     ? cdcMatchData["thirdInningsTeamID"]
     : cdcMatchData["fourthInningsTeamID"];
     ballData["teamName"] =
     inning === "1"
     ? cdcMatchData["firstInnings"]
     : inning === "2"
     ? cdcMatchData["secondInnings"]
     : inning === "3"
     ? cdcMatchData["thirdInnings"]
     : cdcMatchData["fourthInnings"];
     ballData["teamShortName"] =
     inning === "1"
     ? cdcMatchData["firstInningsShortName"]
     : inning === "2"
     ? cdcMatchData["secondInningsShortName"]
     : inning === "3"
     ? cdcMatchData["thirdInningsShortName"]
     : cdcMatchData["fourthInningsShortName"];
     ballData["isBall"] = x.Isball;
     ballData["runs"] = x.Runs;
     ballData["zad"] = x.ZAD;
     ballData["batsmanID"] = x.Batsman;
     ballData["bowlerID"] = x.Bowler;
     ballData["batsmanName"] = playerIdsWithName[x.Batsman];
     ballData["bowlerName"] = playerIdsWithName[x.Bowler];
     ballData["wicket"] = false;
     // ballData['type'] = (details[x.Detail] === '') ? details[x.Detail] : ''
     ballData["type"] =
     typeof details[x.Detail] != "undefined"
     ? details[x.Detail]
     : x.Detail;
     // ballData['type_raw'] = details[x.Detail];
     ballData["currentDay"] =
     cdcMatchData.currentDay && cdcMatchData.matchType == "Test"
     ? parseInt(cdcMatchData.currentDay)
     : 1;
     ballData["currentSession"] =
     cdcMatchData.currentSession && cdcMatchData.matchType == "Test"
     ? parseInt(cdcMatchData.currentSession)
     : 1;
     ballData["matchType"] = cdcMatchData.matchType
     ? cdcMatchData.matchType
     : 1;
     }
     if (
     i == 0 &&
     cdcMatchData.isLastBall &&
     cdcMatchData.currentInnings == parseInt(inning)
     ) {
     ballData["isLastBall"] = true;
     // } else if (i != 0) {
     // ballData["isLastBall"] = false;
     }
     if (ballData.type == "no ball" || ballData.type == "wide") {
     ballData.isBall = false;
     }
     if (!_.isUndefined(x.Summary)) {
     var batsmen = x.Summary.Batsmen.map(y => {
     return {
     batsmanName: playerIdsWithName[y.Batsman],
     onStrike: _.isUndefined(y.Isonstrike) ? false : y.Isonstrike,
     runs: y.Runs,
     balls: y.Balls,
     fours: y.Fours,
     sixes: y.Sixes
     };
     });
     var bowler = x.Summary.Bowlers.map(z => {
     return {
     bowlerName: playerIdsWithName[z.Bowler],
     overs: z.Overs,
     maidens: z.Maidens,
     runs: z.Runs,
     wickets: z.Wickets
     };
     });
     ballData["summary"] = {
     score: x.Summary.Score,
     over: x.Summary.Over,
     runs: x.Summary.Runs,
     wickets: x.Summary.Wickets,
     batsmen: batsmen,
     bowler: bowler,
     teamShortName:
     inning === "1"
     ? cdcMatchData["firstInningsShortName"]
     : inning === "2"
     ? cdcMatchData["secondInningsShortName"]
     : inning === "3"
     ? cdcMatchData["thirdInningsShortName"]
     : cdcMatchData["fourthInningsShortName"]
     };
    
     // BattingTeam
     }
    
     if (!_.isUndefined(x.Iswicket) && x.Iswicket) {
     ballData["wicket"] = x.Iswicket;
     ballData["dismissed"] = playerIdsWithName[x.Dismissed];
     ballData["wicketType"] =
     typeof wickets_ty[x.Dismissal_Type] != "undefined"
     ? wickets_ty[x.Dismissal_Type]
     : x.Dismissal_Type;
     } else {
     ballData["wicket"] = false;
     delete ballData["dismissed"];
     delete ballData["wicketType"];
     }
    
     if (!_.isUndefined(x.Extras_Type) && x.Extras_Type) {
     if (x.Extras_Type == "B") {
     ballData["type"] = "bye";
     } else if (x.Extras_Type == "LB") {
     ballData["type"] = "leg bye";
     } else if (x.Extras_Type == "NB") {
     ballData["type"] = "no ball";
     } else if (x.Extras_Type == "WD") {
     ballData["type"] = "wide";
     }
    
     ballData["extras"] = x.Extras_Runs ? parseInt(x.Extras_Runs) : 0;
     ballData["runs"] = ballData.runs
     ? parseInt(ballData.runs) + ballData.extras
     : 0;
     ballData["runs"] = ballData["runs"].toString();
     }
    
     if (x.Isboundary) {
     let type = x.Runs === "4" ? "four" : "six";
     ballData["type"] = !_.isUndefined(details[x.Detail])
     ? details[x.Detail]
     : !_.isUndefined(x.Isboundary)
     ? type
     : "";
     }
     return ballData;
     });
    
     var inningCommentaryRevamp = data[commentaryKey]
     .slice(0, 10)
     .map((x, i, array) => {
     let ballData = {};
     if (x.Isball) {
     ballData["ballType"] = "ball";
     ballData["timestamp"] = Date.parse(x.Timestamp);
     ballData["isCommentary"] = true;
     ballData["matchID"] = cdcMatchData.matchID;
     ballData["ballNumber"] = x.Over.split(".")[1];
     ballData["innings"] = inning;
     ballData["over"] = x.Over;
     ballData["overNumber"] = (
     Number(x.Over.split(".")[0]) + 1
     ).toString();
     ballData["commentary"] = x.Commentary;
     ballData["teamID"] =
     inning === "1"
     ? cdcMatchData["firstInningsTeamID"]
     : inning === "2"
     ? cdcMatchData["secondInningsTeamID"]
     : inning === "3"
     ? cdcMatchData["thirdInningsTeamID"]
     : cdcMatchData["fourthInningsTeamID"];
     ballData["teamName"] =
     inning === "1"
     ? cdcMatchData["firstInnings"]
     : inning === "2"
     ? cdcMatchData["secondInnings"]
     : inning === "3"
     ? cdcMatchData["thirdInnings"]
     : cdcMatchData["fourthInnings"];
     ballData["teamShortName"] =
     inning === "1"
     ? cdcMatchData["firstInningsShortName"]
     : inning === "2"
     ? cdcMatchData["secondInningsShortName"]
     : inning === "3"
     ? cdcMatchData["thirdInningsShortName"]
     : cdcMatchData["fourthInningsShortName"];
     ballData["runs"] = x.Runs;
     ballData["zad"] = x.ZAD;
     ballData["batsmanID"] = x.Batsman;
     ballData["bowlerID"] = x.Bowler;
     ballData["batsmanName"] = playerIdsWithName[x.Batsman];
     ballData["bowlerName"] = playerIdsWithName[x.Bowler];
     ballData["wicket"] = false;
     // ballData['type'] = (details[x.Detail] === '') ? details[x.Detail] : ''
     ballData["type"] = x.Detail;
     // ballData['type_raw'] = details[x.Detail];
     ballData["currentDay"] =
     cdcMatchData.currentDay && cdcMatchData.matchType == "Test"
     ? parseInt(cdcMatchData.currentDay)
     : 1;
     ballData["currentSession"] =
     cdcMatchData.currentSession && cdcMatchData.matchType == "Test"
     ? parseInt(cdcMatchData.currentSession)
     : 1;
     ballData["matchType"] = cdcMatchData.matchType
     ? cdcMatchData.matchType
     : 1;
    
     if (
     i == 0 &&
     cdcMatchData.isLastBall &&
     cdcMatchData.currentInnings == parseInt(inning)
     ) {
     ballData["isLastBall"] = true;
     } else if (i != 0) {
     ballData["isLastBall"] = false;
     }
     if (!_.isUndefined(x.Summary)) {
     var batsmen = x.Summary.Batsmen.map(y => {
     return {
     batsmanName: playerIdsWithName[y.Batsman],
     onStrike: _.isUndefined(y.Isonstrike) ? false : y.Isonstrike,
     runs: y.Runs,
     balls: y.Balls,
     fours: y.Fours,
     sixes: y.Sixes
     };
     });
     var bowler = x.Summary.Bowlers.map(z => {
     return {
     bowlerName: playerIdsWithName[z.Bowler],
     overs: z.Overs,
     maidens: z.Maidens,
     runs: z.Runs,
     wickets: z.Wickets
     };
     });
     ballData["summary"] = {
     score: x.Summary.Score,
     over: x.Summary.Over,
     runs: x.Summary.Runs,
     wickets: x.Summary.Wickets,
     batsmen: batsmen,
     bowler: bowler,
     teamShortName:
     inning === "1"
     ? cdcMatchData["firstInningsShortName"]
     : inning === "2"
     ? cdcMatchData["secondInningsShortName"]
     : inning === "3"
     ? cdcMatchData["thirdInningsShortName"]
     : cdcMatchData["fourthInningsShortName"]
     };
     // BattingTeam
     }
     if (!_.isUndefined(x.Iswicket) && x.Iswicket) {
     ballData["wicket"] = x.Iswicket;
     ballData["dismissed"] = playerIdsWithName[x.Dismissed];
     ballData["wicketType"] =
     typeof wickets_ty[x.Dismissal_Type] != "undefined"
     ? wickets_ty[x.Dismissal_Type]
     : x.Dismissal_Type;
     } else {
     ballData["wicket"] = false;
     delete ballData["dismissed"];
     delete ballData["wicketType"];
     }
     if (!_.isUndefined(x.Extras_Type) && x.Extras_Type) {
     ballData["type"] = x.Extras_Type;
     ballData["extras"] = x.Extras_Runs ? parseInt(x.Extras_Runs) : 0;
     ballData["runs"] = ballData.runs
     ? parseInt(ballData.runs) + ballData.extras
     : 0;
     ballData["runs"] = ballData["runs"].toString();
     }
     
     return ballData;
     } else {
     ballData["ballType"] = "extra";
     ballData["timestamp"] = Date.parse(x.Timestamp);
     ballData["isCommentary"] = false;
     ballData["commentary"] = x.Commentary;
     return ballData;
     }
     });
     let ballRevamp = await Promise.all(inningCommentaryRevamp);
     
     if (ballRevamp.length) {
     for (let i of ballRevamp) {
     // console.log(i, "commentary");
     await redisSet(cdcMatchData.matchID, i, db);
     }
     // let params = {
     // Body: JSON.stringify(ball),
     // Bucket: process.env.PARSEDBUCKET,
     // Key:
     // "ballbyball/" +
     // cdcMatchData["matchID"] +
     // "/" +
     // (+new Date()).toString() +
     // ".json" // create folder and file name properly
     // };
     // let update = await Commentary.bulkWrite(commentaryArray)
     // await CommentaryRevamp.bulkWrite(commentaryRevampArray);
     // let commentaryDump = await CommentaryDump.updateOne({matchID: cdcMatchData.matchID, 'GameCode': data.GameCode, 'InningNo': data.InningNo}, {$set: data}, {upsert: true})
     // await awsfun('putObject', params)
     // if (cdcMatchData.matchType == 'Test'){
     // await redisDeleteByPattern(`GET_BALL_BY_BALL:${cdcMatchData.matchID}:${cdcMatchData.currentDay}:${cdcMatchData.currentSession}`);
     // await redisDeleteByPattern(`OVER_BY_OVER:${cdcMatchData.matchID}:${cdcMatchData.currentDay}:${cdcMatchData.currentSession}`);
     // } else {
     // await redisDeleteByPattern(`GET_BALL_BY_BALL:${cdcMatchData.matchID}:${cdcMatchData.currentInnings}`);
     // await redisDeleteByPattern(`OVER_BY_OVER:${cdcMatchData.matchID}:${cdcMatchData.currentInnings}`);
     // }
     // if (update) return 'Done'
     if (true) return "Done";
     } else {
     return "something is wrong, please check the code";
     }
     } else {
     return "No Match Found";
     }
     } catch (e) {
     throw e;
     }
    };
    
    const inningsSummaryResolver = async (matchID, innings, db) => {
     try {
     let matchCollection = db.collection("matches");
     let match = await matchCollection.findOne({ matchID: matchID });
     ScoreBoard = match.matchScoreBoard ? match.matchScoreBoard : [];
     let inningsNo = parseInt(innings);
     inningsNo = inningsNo - 1;
     matchScoreBoard = ScoreBoard[inningsNo] ? [ScoreBoard[inningsNo]] : null;
     batting = matchScoreBoard ? matchScoreBoard[0].batting : [];
     bowling = matchScoreBoard ? matchScoreBoard[0].bowling : [];
     battingsort =
     batting &&
     batting.map(x => ({
     ...x,
     playerMatchRuns: isNaN(parseInt(x.playerMatchRuns))
     ? 0
     : parseInt(x.playerMatchRuns)
     }));
     battingList = _.sortBy(battingsort, "playerMatchRuns")
     .reverse()
     .slice(0, 2);
     bowlingList = bowling
     .sort((a, b) =>
     a.playerWicketsTaken < b.playerWicketsTaken
     ? 1
     : a.playerWicketsTaken === b.playerWicketsTaken
     ? a.playerRunsConceeded > b.playerRunsConceeded
     ? 1
     : -1
     : -1
     )
     .slice(0, 2);
     score = matchScoreBoard ? matchScoreBoard[0].total : "";
     score["battingTeamName"] = matchScoreBoard
     ? matchScoreBoard[0].battingTeamName
     : "";
     score["battingTeamID"] = matchScoreBoard
     ? matchScoreBoard[0].battingTeamID
     : "";
    
     finaldata = {
     ballType: "IS",
     score,
     battingList,
     bowlingList
     };
     return finaldata;
     } catch (error) {
     throw error;
     }
    };
    
    const redisSet = async (matchId, comm, db) => {
     const setKey = `Commentaryv2:${matchId}`;
     const args = [setKey, comm.timestamp, comm.timestamp];
     const setElement = await client.zrevrangebyscore(args);
     if (setElement && setElement.length > 0) {
     const parsedComm = JSON.parse(setElement[0]);
     if (comm.isCommentary && parsedComm.runs !== `${comm.runs}${comm.type}`) {
     let ovrBall = {
     over: comm.over,
     runs: `${comm.runs}${comm.type}`
     };
     await setOverbyOver(ovrBall, matchId, comm.innings, "update");
     }
     const rem = [setKey, comm.timestamp, comm.timestamp];
     const rr = await client.zremrangebyscore(rem);
     // if (!rr) {
     // if (comm.isCommentary) {
     // let ovrBall = {
     // over: comm.over,
     // runs: `${comm.runs}${comm.type}`
     // };
     // await setOverbyOver(ovrBall, matchId, comm.innings, "insert");
     // }
     // }
     } else {
     if (comm.isCommentary) {
     let ovrBall = {
     over: comm.over,
     runs: `${comm.runs}${comm.type}`
     };
     await setOverbyOver(ovrBall, matchId, comm.innings, "insert");
     if (comm.isLastBall) {
     const inningsSum = await inningsSummaryResolver(
     comm.matchID,
     comm.innings,
     db
     );
     await setOverbyOver(inningsSum, matchId, comm.innings, "summary");
     }
     }
     }
     if (comm.isCommentary) {
     let ball = {
     id: comm.timestamp,
     over: comm.over,
     ballType: "ball",
     type: comm.type,
     isBall: true,
     runs: `${comm.runs}${comm.type}`,
     zad: comm.zad,
     bowlerName: comm.bowlerName,
     batsmanName: comm.batsmanName,
     commentary: comm.commentary,
     wicket: comm.wicket
     };
     await client.zadd(setKey, comm.timestamp, JSON.stringify(ball));
     if (!!comm.summary) {
     let summary = {
     id: comm.timestamp + 1,
     ballType: "summary",
     score: comm.summary.score,
     teamShortName: comm.summary.teamShortName,
     batsmen: comm.summary.batsmen,
     bowler: comm.summary.bowler,
     // balls: await getOver(comm)
     balls: await getOverBalls(comm.timestamp, setKey)
     };
     const rem = [setKey, comm.timestamp + 1, comm.timestamp + 1];
     await client.zremrangebyscore(rem);
     await client.zadd(setKey, comm.timestamp + 1, JSON.stringify(summary));
     }
     if (comm.isLastBall) {
     const inningsSum = await inningsSummaryResolver(
     comm.matchID,
     comm.innings,
     db
     );
     console.log(inningsSum, "innings Summary");
     const rem = [setKey, comm.timestamp + 2, comm.timestamp + 2];
     await client.zremrangebyscore(rem);
     await client.zadd(setKey, comm.timestamp + 2, JSON.stringify(inningsSum));
     }
     } else {
     let extra = {
     id: comm.timestamp,
     ballType: "extra",
     commentary: comm.commentary
     };
     await client.zadd(setKey, comm.timestamp, JSON.stringify(extra));
     }
    };
    
    const getOverBalls = async (ts, key) => {
     const args = [key, ts, "-inf", "LIMIT", 0, 20];
     const last20 = await client.zrevrangebyscore(args);
     console.log(last20, "last 20 balls");
     if (last20) {
     let firstObj = JSON.parse(last20[0]);
     let ovrNO = 0;
     let balls = [];
     if (firstObj.isBall && firstObj.over !== "") {
     ovrNO = firstObj.over.split(".")[0];
     }
     for (let i = 0; i < last20.length; i++) {
     const j = JSON.parse(last20[i]);
     if (j.isBall) {
     if (j.over.split(".")[0] === ovrNO) {
     let ovrBall = {
     over: j.over,
     runs: `${j.runs}${j.type}`
     };
     balls.push(ovrBall);
     }
     }
     }
     return balls;
     }
     if (!last20) return [];
    };
    
    // const getOver = async ball => {
    // console.log(ball);
    // try {
    // const overKey = `OverbyOverV2:${ball.matchID}:${ball.innings}`;
    // const overNumber = ball.over.split(".")[0];
    // console.log(overKey, overNumber);
    // const len = await client.llen(overKey);
    // console.log(len, "length", len - (Number(overNumber) + 1));
    // const data = await client.lindex(overKey, len - (Number(overNumber) + 1));
    // console.log(data, "overdata");
    // if (data) {
    // return JSON.parse(data);
    // }
    // } catch {
    // return [];
    // }
    // };
    
    
    // const setOverbyOver = async (ball, matchId, innings,type) => {
     const setOverbyOver = async (ball, matchId, innings, type) => {
     console.log(type);
     try {
     const overKey = `OverbyOverV2:${matchId}`;
     let overNumber = ball.over.split(".")[0];
     const listLen = await client.llen(overKey);
     let redisKey = overKey;
     let obj = ball;
     const args =
     innings > 1 ? [overKey, listLen + overNumber] : [overKey, overNumber];
     const overArr = await client.lindex(args);
     if (overArr && type === "insert") {
     // if (overArr) {
     let lastArr = await client.lpop(overKey);
     let parsedArr = JSON.parse(lastArr);
     let str = obj.over + ":" + obj.runs;
     parsedArr.push(str);
     await client.lpush(overKey, JSON.stringify(parsedArr));
     } else if (!overArr && type === "insert") {
     let str = obj.over + ":" + obj.runs;
     let newOvr = [];
     newOvr.push(str);
     await client.lpush(overKey, JSON.stringify(newOvr));
     }
     
     let incomingObjOvrNo = parseInt(obj.over);
     let redisArr = await client.lindex(redisKey, 0);
     if (redisArr && type==="update") {
     let parseArr = JSON.parse(redisArr)
     let redisOvrNo = await client.llen(redisKey);
     redisOvrNo=Number(redisOvrNo)-1;
     // if(innings>1){
     // redisOvrNo = parseInt(parseArr[0].split(':')[0])
     // }
     if(parseInt(parseArr[0].split(':')[0])>=parseInt(obj.over)){
     redisOvrNo = parseInt(parseArr[0].split(':')[0])
     }
     let index = redisOvrNo - incomingObjOvrNo;
     let arr = await client.lindex(redisKey, index);
     let finalArr = JSON.parse(arr);
     let ff = 0;
     finalArr.slice().reverse().map(async function (item, ind) {
     if (item.split(':')[0] === obj.over && ff === 0) {
     let str = obj.over + ":" + obj.runs;
     finalArr[finalArr.length - 1 - ind] = str
     ff = 1;
     await client.lset(redisKey, index, JSON.stringify(finalArr))
     }
     })
     }
     
     if(type==="summary")
     {
     await client.lpush(overKey,JSON.stringify(obj));
     }
     
     
     } catch {}
     };
     
    exports.commentary = commentary;
