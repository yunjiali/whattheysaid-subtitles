//according to the merge-xxx.json file merge the diskref-dvbsub.xml from one day into a single file

var fs = require('fs'),
    xml2js = require('xml2js'),
    _=require('underscore'),
    S=require('string'),
    async = require('async'),
    jf = require('jsonfile'),
    parser = new xml2js.Parser(),
    moment = require('moment'),
    Iconv = require('iconv').Iconv,
    argv=require('optimist').argv;

if(argv.help){
	console.info("--month The month that you want to merge dvbsub.xml files.");
	return;
	process.exit(0);
}

//check argv
if(!argv.month){
	console.error("--month must be provided");
	return;
	process.exit(0);
}

var filename = "merge/merge-"+argv.month+"-01.json";
var mergeArr = jf.readFileSync(filename);
//console.log(mergeArr);

var dvbsubFilenames = fs.readdirSync("raw");
//console.log(dvbsubFilenames);

async.eachSeries(mergeArr, function(mergeFile, mergeFileCallback){
	var dateStr = mergeFile.date;
	console.log(dateStr);
	var refs = mergeFile.refs;
	var dvbsubFileArr = []; //dvbsub xml files for a certain date
	for(var j=0;j<refs.length;j++){
		var fname = refs[j].toString();
		var dvbsubFilename = getDvbsubFile(fname);
		if(dvbsubFilename)
			dvbsubFileArr.push(dvbsubFilename);
	}
	//console.log(dvbsubFileArr);
	//merge xml file
		//get the first video's length
	var videoLength = 0;
	var nextBegin = 0;
	var charCount = 0;
	var transcript = "";
	var syncArr = [];

	async.eachSeries(dvbsubFileArr,function(dvbsubFile,dvbsubFileCallback){

		var data = fs.readFileSync("raw/"+dvbsubFile);
		parser.parseString(data.toString().replace(/&/g, '&amp;'), function(errDvbsub, result){
			var pArr = result.tt.body[0].div[0].p;
			var leftArr = _.filter(pArr,function(p){return p.span[0].$["tts:textAlign"]==='left'});
			var preCue = null;
			//console.log(leftArr.length);
			if(!pArr || pArr.length  === 0){
				//check which video and add the video length
				videoLength+=3600000;
				dvbsubFileCallback(null);
			}
			else{
			
				for(var k=0;k<leftArr.length;k++){
		        	var p = leftArr[k];
		        	var spanArr = p.span;
		        	if(spanArr){
		        		spanArr.splice(spanArr.length-1,1);
		        		if(spanArr){
		        			var cueText = "";
			        		for(var m=0;m<spanArr.length;m++){
			        			var span = spanArr[m];
			        			cueText+=span._;
			        		}
			        		cueText=S(cueText).trim().s;
			        		if(cueText !==""){

			        			cueText+=" ";
			        			var mid = cueText.length/3;
			        			var commind = cueText.indexOf(",");
			        			var fstopind = cueText.indexOf(".");
			        			if(fstopind !== -1 && fstopind <=mid){
			        				var strippedStr = cueText.substring(0,fstopind+1);
			        				if(preCue){
			        					preCue.text+=strippedStr;
			        					preCue.charLength+=strippedStr.length;
			       						cueText = cueText.substr(fstopind+1);
			       					}
			        			}
			        			else if(commind !== -1 && commind <=mid){
			        				var strippedStr = cueText.substring(0,commind+1);
			        				if(preCue){
			        					preCue.text+=strippedStr;
			        					preCue.charLength+=strippedStr.length;
			       						cueText = cueText.substr(commind+1);
			       					}
			        			}
			        			
			        			var cue = {};

			        			cue.start = nextBegin;
			        			//TODO: startNormalised, endNormalised
			        			//p.$.begin;
			        			nextBegin = moment.duration(p.$.end).as('ms')+videoLength; 
			        			cue.end = moment.duration(p.$.end).as('ms')+videoLength;
			        			cue.text = cueText;
			        			cue.charStart = charCount;
			        			cue.charLength = cueText.length;
			        			charCount+=cueText.length;
			        			transcript+=cueText;
			        			syncArr.push(cue);
			        			preCue = cue;
			        		}

			        		//We need to revise the algorithm a little bit:
			        		//To make elastic search more efficient, we'd better put a full sentence
			        		//or at least a phrase into a cueText, instead of sth broken, such as:
			        		//vote for the budget reduction. That
			        		
		        		}
		        	}
	        	}
	        	var len = moment.duration(pArr[pArr.length-1].$.end).as('ms'); //use moment
				videoLength+=len;

				dvbsubFileCallback(null);
	        }
		});
	},
	function(dvbsubFileErr, results){
		//console.log(transcript);
        //console.log(JSON.stringify(syncArr, null, 4));
		jf.writeFile('json/'+dateStr+'.json',syncArr,
			function(errWriteFile){
				if(errWriteFile){
					console.error('json/'+dateStr+'.json error'+errWriteFile);
				}

				fs.writeFile('transcripts/'+dateStr+'-transcript.txt',transcript,
					function(errWriteFile){
						if(errWriteFile){
							console.error('Write transcripts/'+dateStr+'-transcript.txt error.'+errWriteFile);
						}
						mergeFileCallback(null);
					}
				);
			}
		);
	});
	
	
		//directly copy file
		//fs.createReadStream('raw/'+dvbsubFileArr[0]).pipe(fs.createWriteStream('merge/'+dateStr+".xml"));
	
	//TODO: if error, get video length

	},function(mergeFileErr, mergeFileResults){
		console.log("finish.");
	}
);


function getDvbsubFile(fname){
	//14 chars are the same
	var prefixFile = fname.substring(0,14);
	var dvbsubfile = _.find(dvbsubFilenames, function(dvbsubFilename){
		return dvbsubFilename.substring(0,14) === prefixFile;
	});

	return dvbsubfile;
}

function msToTime(duration) {
    var milliseconds = parseInt((duration%1000)/100)
        , seconds = parseInt((duration/1000)%60)
        , minutes = parseInt((duration/(1000*60))%60)
        , hours = parseInt((duration/(1000*60*60))%24);

    hours = (hours < 10) ? "0" + hours : hours;
    minutes = (minutes < 10) ? "0" + minutes : minutes;
    seconds = (seconds < 10) ? "0" + seconds : seconds;

    return hours + ":" + minutes + ":" + seconds + "." + milliseconds;
}




