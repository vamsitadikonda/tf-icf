#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>

#define MAX_WORDS_IN_CORPUS 32
#define MAX_FILEPATH_LENGTH 16
#define MAX_WORD_LENGTH 16
#define MAX_DOCUMENT_NAME_LENGTH 8
#define MAX_STRING_LENGTH 64

typedef char word_document_str[MAX_STRING_LENGTH];

typedef struct o {
	char word[32];
	char document[8];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
} obj;

typedef struct w {
	char word[32];
	int numDocsWithWord;
	int currDoc;
} u_w;

static int myCompare (const void * a, const void * b)
{
    return strcmp (a, b);
}

int main(int argc , char *argv[]){
	DIR* files;
	struct dirent* file;
	int i,j;
	int numDocs = 0, docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];
	
	// Will hold all TFICF objects for all documents
	obj TFICF[MAX_WORDS_IN_CORPUS];
	int TF_idx = 0;
	
	// Will hold all unique words in the corpus and the number of documents with that word
	u_w unique_words[MAX_WORDS_IN_CORPUS];
	int uw_idx = 0;
	
	// Will hold the final strings that will be printed out
	word_document_str strings[MAX_WORDS_IN_CORPUS];
	
	
	//Count numDocs
	if((files = opendir("input")) == NULL){
		printf("Directory failed to open\n");
		exit(1);
	}
	while((file = readdir(files))!= NULL){
		// On linux/Unix we don't want current and parent directories
		if(!strcmp(file->d_name, "."))	 continue;
		if(!strcmp(file->d_name, "..")) continue;
		numDocs++;
	}
	
	// Loop through each document and gather TFICF variables for each word
	for(i=1; i<=numDocs; i++){
		sprintf(document, "doc%d", i);
		sprintf(filename,"input/%s",document);
		FILE* fp = fopen(filename, "r");
		if(fp == NULL){
			printf("Error Opening File: %s\n", filename);
			exit(0);
		}
		
		// Get the document size
		docSize = 0;
		while((fscanf(fp,"%s",word))!= EOF)
			docSize++;
		
		// For each word in the document
		fseek(fp, 0, SEEK_SET);
		while((fscanf(fp,"%s",word))!= EOF){
			contains = 0;
			
			// If TFICF array already contains the word@document, just increment wordCount and break
			for(j=0; j<TF_idx; j++) {
				if(!strcmp(TFICF[j].word, word) && !strcmp(TFICF[j].document, document)){
					contains = 1;
					TFICF[j].wordCount++;
					break;
				}
			}
			
			//If TFICF array does not contain it, make a new one with wordCount=1
			if(!contains) {
				strcpy(TFICF[TF_idx].word, word);
				strcpy(TFICF[TF_idx].document, document);
				TFICF[TF_idx].wordCount = 1;
				TFICF[TF_idx].docSize = docSize;
				TFICF[TF_idx].numDocs = numDocs;
				TF_idx++;
			}
			
			contains = 0;
			// If unique_words array already contains the word, just increment numDocsWithWord
			for(j=0; j<uw_idx; j++) {
				if(!strcmp(unique_words[j].word, word)){
					contains = 1;
					if(unique_words[j].currDoc != i) {
						unique_words[j].numDocsWithWord++;
						unique_words[j].currDoc = i;
					}
					break;
				}
			}
			
			// If unique_words array does not contain it, make a new one with numDocsWithWord=1 
			if(!contains) {
				strcpy(unique_words[uw_idx].word, word);
				unique_words[uw_idx].numDocsWithWord = 1;
				unique_words[uw_idx].currDoc = i;
				uw_idx++;
			}
		}
		fclose(fp);
	}
	
	// Print TF job similar to HW4/HW5 (For debugging purposes)
	printf("-------------TF Job-------------\n");
	for(j=0; j<TF_idx; j++)
		printf("%s@%s\t%d/%d\n", TFICF[j].word, TFICF[j].document, TFICF[j].wordCount, TFICF[j].docSize);
		
	// Use unique_words array to populate TFICF objects with: numDocsWithWord
	for(i=0; i<TF_idx; i++) {
		for(j=0; j<uw_idx; j++) {
			if(!strcmp(TFICF[i].word, unique_words[j].word)) {
				TFICF[i].numDocsWithWord = unique_words[j].numDocsWithWord;	
				break;
			}
		}
	}
	
	// Print ICF job similar to HW4/HW5 (For debugging purposes)
	printf("------------ICF Job-------------\n");
	for(j=0; j<TF_idx; j++)
		printf("%s@%s\t%d/%d\n", TFICF[j].word, TFICF[j].document, TFICF[j].numDocs, TFICF[j].numDocsWithWord);
		
	// Calculates TFICF value and puts: "document@word\tTFICF" into strings array
	for(j=0; j<TF_idx; j++) {
		double TF = log10( 1.0 * TFICF[j].wordCount / TFICF[j].docSize + 1 );
		double ICF = log10(1.0 * (TFICF[j].numDocs + 1) / (TFICF[j].numDocsWithWord + 1) );
		double TFICF_value = TF * ICF;
		sprintf(strings[j], "%s@%s\t%.16f", TFICF[j].document, TFICF[j].word, TFICF_value);
	}
	
	// Sort strings and print to file
	qsort(strings, TF_idx, sizeof(char)*MAX_STRING_LENGTH, myCompare);
	FILE* fp = fopen("output.txt", "w");
	if(fp == NULL){
		printf("Error Opening File: output.txt\n");
		exit(0);
	}
	for(i=0; i<TF_idx; i++)
		fprintf(fp, "%s\n", strings[i]);
	fclose(fp);
	
	return 0;	
}
