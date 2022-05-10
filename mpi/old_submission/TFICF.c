/*
Single Author info:
btadiko Bapiraju Vamsi Tadikonda
*/
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>
#include<mpi.h>
#include<stddef.h>

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
	//************************* Phase 0 *******************************

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

	// Initialize openMPI
    MPI_Init(&argc, &argv);
    //Define variables 
    int numproc,rank;
    MPI_Status status;

    // get the number of procs in the comm
    MPI_Comm_size(MPI_COMM_WORLD, &numproc);
    // get my rank in the comm 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // Creating mpi_u_w custom datatype for sending messages between tasks
    MPI_Datatype mpi_u_w;
	int blocklengths[3] = {32,1,1};
	MPI_Datatype types[3] = {MPI_CHAR, MPI_INT, MPI_INT};
	MPI_Aint offsets[3] = {offsetof(u_w, word), offsetof(u_w, numDocsWithWord),offsetof(u_w, currDoc)};
 	MPI_Type_create_struct(3, blocklengths, offsets, types, &mpi_u_w);
    MPI_Type_commit(&mpi_u_w);    
    // Creating mpi_string custom datatype to send final string messages
	MPI_Datatype mpi_string;
	int blocklen[1]={MAX_STRING_LENGTH};
	MPI_Datatype type[1]={MPI_CHAR};
	MPI_Aint offset[1]={0};
	MPI_Type_create_struct(1, blocklen, offset, type, &mpi_string);
    MPI_Type_commit(&mpi_string);

	//************************* Phase 1 *******************************
    if(rank!=0){
    	// Worker Tasks
		// Loop through each document and gather TFICF variables for each word
    	for(i=rank;i<=numDocs;i+=numproc-1)
    	{
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
    	//Sending the count of unique words to Root node.
    	MPI_Send(&uw_idx,1,MPI_INT,0,0,MPI_COMM_WORLD);
    	//Sending the set of unique words to Root node.
    	MPI_Send(unique_words,MAX_WORDS_IN_CORPUS,mpi_u_w,0,0,MPI_COMM_WORLD);
    }
    else{ // Root Task
    	// creating a temporary array to recieve unique words from workers
    	u_w *temp_u_w = (u_w*) malloc(sizeof(u_w) * MAX_WORDS_IN_CORPUS);
        for(int r=1;r<numproc;r++){
        	int temp_uw_idx;
        	MPI_Recv(&temp_uw_idx,1,MPI_INT,r,0,MPI_COMM_WORLD,&status); // Recieving the count of unique words
        	MPI_Recv(temp_u_w,MAX_WORDS_IN_CORPUS,mpi_u_w,r,0,MPI_COMM_WORLD,&status); // recieving the array of unique words
			for(i=0;i<temp_uw_idx;i++){
					contains = 0;
					// If unique_words array already contains the word, just add the two numDocsWithWord
					for(j=0; j<uw_idx; j++) {
						if(!strcmp(unique_words[j].word, temp_u_w[i].word)){
							contains = 1;
							unique_words[j].numDocsWithWord = temp_u_w[i].numDocsWithWord + unique_words[j].numDocsWithWord;
							break;
						}
					}
					// If unique_words array does not contain it, make a new one with numDocsWithWord of the temp array
					if(!contains) {
						strcpy(unique_words[uw_idx].word, temp_u_w[i].word);
						unique_words[uw_idx].numDocsWithWord = temp_u_w[i].numDocsWithWord;
						unique_words[uw_idx].currDoc = temp_u_w[i].currDoc;
						uw_idx++;
					}
				
			}        
        }
    }
	MPI_Barrier(MPI_COMM_WORLD);
	//Broadcasting the final count of unique words
	MPI_Bcast(&uw_idx,1,MPI_INT,0,MPI_COMM_WORLD);
	//Broadcasting the final array of unique words
	MPI_Bcast(unique_words,uw_idx,mpi_u_w,0,MPI_COMM_WORLD);

	//************************* Phase 2 *******************************

	if(rank!=0){
		// Worker Task
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
		//Sending the count of output strings to the root task
		MPI_Send(&TF_idx,1,MPI_INT,0,0,MPI_COMM_WORLD);
		//Sending the array of output strings to the root task
		MPI_Send(strings,TF_idx,mpi_string,0,0,MPI_COMM_WORLD);
	}		
	else{
		// Root Task
		// Recieving all the strings from workers
		TF_idx=0;
		// Creating temporary array to recieve output strings
		word_document_str temp_strings[MAX_WORDS_IN_CORPUS]; 
		for(int r=1;r<numproc;r++){
			int temp_idx;
			MPI_Recv(&temp_idx,1,MPI_INT,r,0,MPI_COMM_WORLD,&status); //Recieving the number of output strings
        	MPI_Recv(&temp_strings,temp_idx,mpi_string,r,0,MPI_COMM_WORLD,&status); //Recieving the array of output strings
        	// Adding all the recieved output strings from workers to strings array
        	for(i=0;i<temp_idx;i++){
        		strcpy(strings[TF_idx],temp_strings[i]);
        		TF_idx++;
        	}
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
	}
    MPI_Finalize();
    return 0;
}
