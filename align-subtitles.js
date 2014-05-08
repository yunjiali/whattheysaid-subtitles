//algorithm is as follows
//1. check merge/date.json and get the text
//2. search elasticsearch index in a specific date
//3. return the gids
//4. clean the gidsArr by looking for neibourhood similarity
//5. look back the arry in date.json and get the start time for each cue
//6. write it back to debate database
var CONFIG=require('config').Crawler;
var ElasticSearchClient = require('elasticsearchclient'),
	fs=require('fs'),
	_=require('underscore'),
	async=require('async'),
	S=require('string'),
	mongo = require('mongodb'),
	ObjectID = mongo.ObjectID,
	monk = require('monk'),
	jf = require('jsonfile');


var serverOptions = {
    host: CONFIG.elasticSearchHost,
    port: CONFIG.elasticSearchPort,
    secure: false
}


var elasticSearchClient = new ElasticSearchClient(serverOptions);
console.log("connect to elasticsearchclient at "+CONFIG.elasticSearchHost+":"+CONFIG.elasticSearchPort);

var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
//remove old index
//elasticSearchClient.delete({"_index":"debates","_type" : "debate"});

//search a string specifying the date to look for
/*var qryObj = {
	"query":{
		"filtered":{
			"query":{
				"match" : {
			    	"plain_text":{
			    		"query":"him that in the past, children used to turn right to work in the shipyards or left to work in the coal mines, now they might as well walk into the sea.",
			    		"operator":"and"
			    	}
			    }
			},
		    "filter":{
		    	"term":{"hdate":"2013-03-04"}
		    }
		}
	}
};

elasticSearchClient.search("debates","debate",qryObj, function(err,data){
	var result = JSON.parse(data);
	console.log(result);
});*/

var jsonFilenames = fs.readdirSync("json");
async.eachSeries(jsonFilenames, function(jsonFilename, jsonFilenameCallback){
	console.log(jsonFilename);
	if(jsonFilename.indexOf(".DS") !==-1)
		return jsonFilenameCallback(null);

	var rawCueArr = jf.readFileSync("json/"+jsonFilename);
	var hdate = jsonFilename.split(".json")[0];
	var cuesAlignedArr = []

	async.eachSeries(rawCueArr, function(cue, cueCallback){
		var qryObj = {
			"query":{
				"filtered":{
					"query":{
						"match" : {
					    	"plain_text":{
					    		"query":S(cue.text).trim().s,
					    		"operator":"and"			    	
					    	}
					    }
					},
				    "filter":{
				    	"term":{"hdate":hdate}
				    }
				}
			}
		};
		elasticSearchClient.search("debates","debate",qryObj, function(esErr,esData){
			var result = JSON.parse(esData);
			if(result.hits.hits.length){
				var resultArr = [];
				for(var i=0;i<result.hits.hits.length;i++){
					resultArr.push(result.hits.hits[i]._source.gid);
				}
				cuesAlignedArr.push(resultArr);

			}else{
				cuesAlignedArr.push(["0"]);
			}
			
			cueCallback(null);
		});

	},function(cueErr, cueResults){
		//start to clean up data
		//check neighourhood (+-20 cues), and return the weight
		var cleanCuesArr = []
		//first round of cleaning the data
		for(var j=0;j<cuesAlignedArr.length;j++){
			var cueArr = cuesAlignedArr[j];
			var cueWeightedArr = [];
			var rangeStart = j-20<0?0:j-20; 
			var rangeEnd = (j+20>cuesAlignedArr.length-1)?cuesAlignedArr.length-1:j+20;
			var checkRange = _.range(rangeStart, rangeEnd);
			for(var k=0;k<cueArr.length;k++){
				var cue = {}
				cue.gid = cueArr[k];
				cue.weight = 1;
				for(var m=0;m<checkRange.length;m++){
					var checkedCueArr = cuesAlignedArr[checkRange[m]];
					if(_.find(checkedCueArr, function(c){return c === cue.gid;})){
						cue.weight*=100;
					}
				}
				cueWeightedArr.push(cue);
			}
			var maxWeightedCue =_.max(cueWeightedArr,function(c){return c.weight;});
			if(maxWeightedCue.weight === 1){
				cleanCuesArr.push("0");
			}
			else
				cleanCuesArr.push(maxWeightedCue.gid);
		}

		db.get("debates").find({"hdate":hdate}, function(debateErr, debates){
			var alignedGidArr = [];

			//second round of cleaning the data
			for(var j=0;j<debates.length;j++){
				var currentGid = debates[j].gid;
				var currentEpid = debates[j].epobject_id;
				var currentStartCharPos = debates[j].start_char_pos;
				var currentCharCount = debates[j].char_count;
				var cueArr = []
				for(var k=0;k<cleanCuesArr.length;k++){
					if(cleanCuesArr[k]===currentGid){
						var cue = {};
						cue.epobject_id = currentEpid;
						cue.gid = currentGid;
						cue.start = rawCueArr[k].start;
						cue.end = rawCueArr[k].end;
						cue.index = k;
						cueArr.push(cue);
					}
				}
				if(cueArr.length>1){
					for(var m=1;m<cueArr.length;m++){
						if(cueArr[m].start-cueArr[m-1].start > 60000){ //they are far away from each other
							//not in the correct position
							cueArr[m-1].gid = "0";
							if(m=cueArr.length-1)
								cueArr[m].gid = "0";
						}
					}

					//console.log(cueArr);
					var newCueArr = _.filter(cueArr, function(c){return c.gid !== "0"});
					if(newCueArr.length === 0)
						alignedGidArr.push({start_char_pos: currentStartCharPos, char_count:currentCharCount,epobject_id:currentEpid, gid:currentGid, start:-1, end:-1});
					else{
						alignedGidArr.push({start_char_pos: currentStartCharPos, char_count:currentCharCount,epobject_id:currentEpid, gid:currentGid, start:newCueArr[0].start, end:newCueArr[newCueArr.length-1].end});
					}
				}
				else if(cueArr.length ===1){
					alignedGidArr.push({start_char_pos: currentStartCharPos, char_count:currentCharCount,epobject_id:currentEpid, gid:currentGid, start:cueArr[0].start, end:cueArr[0].end})
				}
				else{
					alignedGidArr.push({start_char_pos: currentStartCharPos, char_count:currentCharCount,epobject_id:currentEpid, gid:currentGid, start:-1, end:-1})
				}
			}

			alignedGidArr = _.sortBy(alignedGidArr, function(c){return parseInt(c.epobject_id);});
			//third round of clean
			var emptyAndWrongArr = []; //the array to save empty and wrong aligned gid
			for(var j=0;j<alignedGidArr.length;j++){
				var gid = alignedGidArr[j];
				var preGid = null;
				if(gid.start === -1){
					emptyAndWrongArr.push(gid);
				}
				else if(preGid && (gid.start-preGid.start > 120000)){
					emptyAndWrongArr.push(gid);
				}
				//else if(!preGid && gid.start > 1800000){ //the programme has started for 30mins, but no gid has aligned
				//	emptyAndWrongArr.push(gid);
				//} 
				else if(gid.start !== -1 && emptyAndWrongArr.length >0){
					//set the start value for all the gids in emptyAndWrongArr
					var preStart = 0; //the start time of the previous
					if(preGid !==  null){ //missing the start time from the first gid
						preStart = preGid.start;
					}
					//distribute the time buy character count
					var timespan = gid.start - preStart;
					var totalChar = preGid?preGid.char_count:0;
					for(var k=0;k<emptyAndWrongArr.length;k++){
						totalChar+=emptyAndWrongArr[k].char_count;
					}

					var addedCharCount = preGid?preGid.char_count:0;
					for(var k=0;k<emptyAndWrongArr.length;k++){
						addedCharCount+=emptyAndWrongArr[k].char_count;
						emptyAndWrongArr[k].start = parseInt(preStart+timespan*(addedCharCount/totalChar));
					}

					for(var k=0;k<emptyAndWrongArr.length;k++){
						if(k<emptyAndWrongArr.length-1)
							emptyAndWrongArr[k].end = emptyAndWrongArr[k+1].start;
						else
							emptyAndWrongArr[k].end =  gid.start;
					}
					emptyAndWrongArr=[];
				}
				else //cue with correct start time, set its position
					preGid = gid;
			}

			//TODO: fourth round, check the start and end time see if there are any weired things
			
			/*jf.writeFile('aligned/'+hdate+'.json',alignedGidArr,
				function(errWriteFile){
					if(errWriteFile){
						console.error('json/'+dateStr+'.json error'+errWriteFile);
					}
					jsonFilenameCallback(null);
				}
			);*/
			
			async.eachSeries(alignedGidArr, function(gid, debateCallback){
				db.get("debates").findAndModify({"gid":gid.gid}, 
					{$set:{star_time: gid.start, end_time:gid.end}},
					function(modifyErr, d){
						console.log(d.gid+" updated.");
						debateCallback(null);
					}
				);
			}, function(debateErr, debateResults){
				jsonFilenameCallback(null);
			});
		});
	});		
},function(jsonFilenameErr, jsonFilenameResults){
	console.log("finished.");
});
