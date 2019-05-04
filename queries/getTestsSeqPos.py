import os
import xml.dom.minidom as minidom
import xml
import pandas as pd

BASE_DIR=os.getcwd()
BASE_DIR=BASE_DIR+'\\data\\course'

def getListsOfProblemsVertical():
    finalExam=''
    DATA_DIR=BASE_DIR+'\\vertical'
    filesList=os.listdir(path=DATA_DIR)
    gradedProblemsArray=[]
    ungradedProblemsArray=[]

    for filename in filesList:
        xmldoc = minidom.parse(DATA_DIR+'\\'+filename)
        node = xmldoc.documentElement
        isGraded=False
        
        if node.nodeType == xml.dom.Node.ELEMENT_NODE:
            for (name, value) in node.attributes.items():                
                #print(' Attr -- имя: %s значение: %s' % (name, value))
                if name=='format' and value=='Final Exam':
                    finalExam=filename[:-4]
                if name=='graded' and value=='true':
                    isGraded=True
                    gradedProblemsArray.append(filename[:-4])
                    
        if not isGraded:
            rem = xmldoc.getElementsByTagName('vertical')
            if xmldoc.getElementsByTagName('problem'):
                problem_id=xmldoc.getElementsByTagName('problem')[0].getAttribute("url_name")
                ungradedProblemsArray.append([filename[:-4],problem_id])
    return gradedProblemsArray,ungradedProblemsArray,finalExam     

def getListsOfProblemsSequential():
    gradedProblemsVertArray,ungradedProblemsVertArray, finalExamVert = getListsOfProblemsVertical()
    ungradedProblemsDF=pd.DataFrame(ungradedProblemsVertArray,columns=['vert_id','problem_id'])
    DATA_DIR=BASE_DIR+'\\sequential'
    filesList=os.listdir(path=DATA_DIR)
    gradedProblemsSeqArray=[]
    ungradedProblemsSeqArray=[]
    finalExamSeq=[]
    for filename in filesList:
        xmldoc = minidom.parse(DATA_DIR+'\\'+filename)
        node = xmldoc.documentElement
        rem = xmldoc.getElementsByTagName('sequential')
        verticalArray=xmldoc.getElementsByTagName('vertical')
        position=0
        
        for vertical in verticalArray:
            attrib=vertical.attributes.items()
            currentVertName=attrib[0][1]
            position+=1
            if currentVertName in gradedProblemsVertArray:
                tmpDict = [filename[:-4], position]
                gradedProblemsSeqArray.append(tmpDict)
            elif currentVertName in list(ungradedProblemsDF['vert_id']):
                problem_id=list(ungradedProblemsDF.loc[ungradedProblemsDF['vert_id']==currentVertName]['problem_id'])[0]
                tmpDict = [filename[:-4], position,problem_id]
                ungradedProblemsSeqArray.append(tmpDict)
            elif currentVertName ==finalExamVert:
                finalExamSeq = {filename[:-4]: position}
    
    return gradedProblemsSeqArray, ungradedProblemsSeqArray, finalExamSeq