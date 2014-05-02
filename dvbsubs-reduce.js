//reduce the original dvbsubs file and export two files:
//1. a json index file about start/end character and start/end time of each sentence
//2. a txt file with all concat txt from dvbsubs

var fs = require('fs'),
    xml2js = require('xml2js'),
    _=require('underscore'),
    S=require('string'),
    jf = require('jsonfile');

var parser = new xml2js.Parser();
var charCount = 0;
var transcript = "";
var syncArr = [];
var nextBegin = 0;
fs.readFile( 'test/2013-02-13-part2.xml', function(err, data) {
    parser.parseString(data, function (err, result) {
        //console.log(JSON.stringify(result.tt.body.div,null,4));
        //console.dir(result.tt.body[0].div[0].p[1].span[0].$["tts:textAlign"]);
        var pArr = result.tt.body[0].div[0].p;
        var leftArr = _.filter(pArr,function(p){return p.span[0].$["tts:textAlign"]==='left'});
        //console.log(leftArr.length);
        //console.log(leftArr[1]);

        for(var i=0;i<leftArr.length;i++){
        	var p = leftArr[i];
        	var spanArr = p.span;
        	if(spanArr){
        		spanArr.splice(spanArr.length-1,1);
        		if(spanArr){
        			var cueText = "";
	        		for(var j=0;j<spanArr.length;j++){
	        			var span = spanArr[j];
	        			cueText+=span._;
	        		}
	        		cueText=S(cueText).trim().s;
	        		if(cueText !==""){
	        			cueText+=" ";
	        			var cue = {};
	        			cue.start = nextBegin;
	        			//TODO: startNormalised, endNormalised
	        			//p.$.begin;
	        			nextBegin = p.$.end; 
	        			cue.end = p.$.end;
	        			cue.text = cueText;
	        			cue.charStart = charCount;
	        			cue.charLength = cueText.length;
	        			charCount+=cueText.length;
	        			transcript+=cueText;
	        			syncArr.push(cue);
	        		}

	        		//We need to revise the algorithm a little bit:
	        		//To make elastic search more efficient, we'd better put a full sentence
	        		//or at least a phrase into a cueText, instead of sth broken, such as:
	        		//vote for the budget reduction. That
	        		
        		}
        	}
        }

        //console.log(transcript);
        //console.log(JSON.stringify(syncArr, null, 4));
        fs.writeFile('test/2013-02-13-transcript.txt','utf-8',transcript,
			function(errWriteFile){
				if(errWriteFile){
					console.error("Write 2013-02-13-transcript.txt error."+errWriteFile);
				}
			}
		);

		jf.writeFile('test/2013-02-13-index.json',syncArr,
			function(errWriteFile){
				if(errWriteFile){
					console.error("Write 2013-02-13-index.json error."+errWriteFile);
				}
			}
		);
    });
}); 