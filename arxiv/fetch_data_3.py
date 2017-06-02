import urllib
import time

retry_time = 45

def get_raw_data():
	start = time.clock()
	query = "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc"
	print("request: %s" % (query))
	request = urllib.request.Request(query)
	response = urllib2.request.urlopen(request).read()
	rawfile = open('papers_0.xml','w')
	rawfile.write(response)
	rawfile.close()
	
	end = time.clock()
	print("takes: %f s" % (end-start))
	get_resume(response)

	pos_start = response.rfind('<resumptionToken')
	pos_end = response.rfind('</resumptionToken')
	if pos_end > 0 and pos_end > pos_start:
		pos = response.rfind('>', pos_start, pos_end)
		resume_token = response[pos+1:pos_end]
		print("request_resume: %s" % (resume_token))
		get_resume(resume_token)

def get_resume(token):
	time.sleep(retry_time)
	start = time.clock()
	rawfile = open('papers_%s.xml'%(token),'w')
	try:
		query = "http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken=%s" % (token)
		request = urllib.request.Request(query)
		response = urllib.request.urlopen(request).read()
		rawfile.write(response)
		rawfile.close()
		end = time.clock()
		print("takes: %f s" % (end-start))

		pos_start = response.rfind('<resumptionToken')
		pos_end = response.rfind('</resumptionToken')
		if pos_end > 0 and pos_end > pos_start:
			pos = response.rfind('>', pos_start, pos_end)
			resume_token = response[pos+1:pos_end]
			print("request_resume: %s" % (resume_token))
			get_resume(resume_token)
	except Exception as err:
		print(err)
		print("retry resume_token: %s" % (token))
		time.sleep(30)
		get_resume(token)

if __name__ == '__main__':
	get_raw_data()
