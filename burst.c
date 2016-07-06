#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <pthread.h>

#ifdef URL_SUPPORT
#include <curl/curl.h>
#endif

#define FILE_PATH_MAX 512
#define READ_BLOCK_SIZE 4192
#define VERSION "0.0.1"
int workers_pool_size = 10; //default to 10
char output_directory[FILE_PATH_MAX] = {0};
int b_output_dir = 0;
const char *base_file_name = 0;

pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;	
pthread_cond_t count_threshold_cv = PTHREAD_COND_INITIALIZER;
int burst_count = 0;
int terminate_workers = 0;
int current_segment = 0;
int no_more_seg = 0;
mode_t source_file_mode;
struct burst_attr {
	int startingByte;
	int endingByte;
};
struct filename_attr {
	char source_path[FILE_PATH_MAX];
	char suffix[FILE_PATH_MAX];
	char extension[80];
};

struct burst_attr * burst_attr_list = NULL;
int burst_attr_list_size = 16;//default initial
struct burst_attr * getBurstAttrList(void *ptr, unsigned int size)
{
	unsigned int sizeBytes = size * sizeof(struct burst_attr); 
	burst_attr_list = realloc(ptr, sizeBytes);
	return burst_attr_list;
} 

void split_file_by_extension(const char *filename, char *prefix, char *suffix)
{
	const char* ext = strrchr(filename, '.');
	if (!ext) {
		strcpy(prefix, filename);
	} else {
		int bytes = (ext - filename) + 1;
		strncpy(prefix, filename, bytes - 1);
		strcpy(suffix, ext + 1);
	}
}


#ifdef URL_SUPPORT
int downloadURL(const char *url, const char * outfilename)
{  
	CURL *curl;
	FILE *fp;
	CURLcode res;
	curl = curl_easy_init();                                                                                                                                                       
	if (curl)
	{
		fp = fopen(outfilename,"wb");
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);
		fclose(fp);
		return 1;
	}
	return 0;	
}
#endif

char *basename(char const *path)
{
	char *s = strrchr(path, '/');
	if(s==NULL) {
		return strdup(path);
	} else {
		return strdup(s + 1);
	}
}

/*
@brief: Checks if the source file is a valid file or not
*/
int validate_file(const char *file_path)
{
	struct stat buf;
	if (stat(file_path, &buf) < 0)
	{
		perror("validate_file : ");
		return 0;
	}
	source_file_mode = buf.st_mode;
	if (!S_ISREG(buf.st_mode))
		return 0;
	return 1;
}

/*
@brief: init the attribute list
*/
void init_burst_attr_list()
{
	burst_attr_list = getBurstAttrList(NULL, burst_attr_list_size/*initial default size*/);	
}

/*
@brief: prints attribute list ; for debugging
*/
void print_burst_attr_list(int size)
{
	int i = 0;
	while (i < size) {
		printf("segment %d, startingByte : %d, endingByte : %d\n",i+1, burst_attr_list[i].startingByte, burst_attr_list[i].endingByte);
		++i;
	}
} 


/*
@brief: burst worker for creating a burst segment file from the specified attribute
*/
void* burst_worker(void *arg)
{
	const struct filename_attr *name = arg;
	pthread_t tid = pthread_self();
	//printf("worker created with pid : %lu\n", tid);
	while (!terminate_workers)
	{
		struct burst_attr *attr;
		pthread_mutex_lock(&count_mutex);	
		while (current_segment <= burst_count) {	
			pthread_cond_wait(&count_threshold_cv,&count_mutex);
			if (terminate_workers) {
				pthread_mutex_unlock(&count_mutex);
				return NULL;
			}
		}
		attr = &burst_attr_list[burst_count++];
		if (no_more_seg && burst_count == current_segment) {
			//this is the last segment 
			//wake up all workers
			terminate_workers = 1;
			pthread_mutex_unlock(&count_mutex);
			pthread_cond_broadcast(&count_threshold_cv);
		}
		else {
			pthread_mutex_unlock(&count_mutex);
		}
		
		if (attr->startingByte == attr->endingByte) {
			continue;
		}
		
		//printf("%s : { %d, %d }\n", attr->name, attr->startingByte, attr->endingByte);
	
		//open source file
		int fd_src = open(name->source_path, O_RDONLY);
		if (fd_src < 0) {
			perror("burst_worker open source file");
			exit(-1);
		}
		//create segment file
		char segmentname[FILE_PATH_MAX] = {0};
		sprintf(segmentname,"%s%d%s%s", name->suffix, burst_count, strlen(name->extension) ? ".": "", name->extension);
		int fd_seg = open (segmentname, O_RDWR | O_CREAT, source_file_mode);
		if (fd_seg < 0) {
			perror("burst_worker open and create segment");
			exit(-1);
		}
		//seek to starting byte
		lseek(fd_src, attr->startingByte, SEEK_CUR);
		int bytesToCopy = (attr->endingByte - attr->startingByte) + 1;
		int blockSize = bytesToCopy > READ_BLOCK_SIZE ? READ_BLOCK_SIZE : bytesToCopy;
		char *buf = malloc(blockSize);
		int count = 0;
		int n = 0;
		//copy block by block
		while(count < bytesToCopy && (n=read(fd_src, buf, blockSize)) > 0) {
			write(fd_seg, buf, n);
			count += n;
			blockSize = (bytesToCopy - count) < blockSize ? (bytesToCopy - count) : blockSize;
		}
		free(buf);
		close(fd_src);
		close(fd_seg);
	}
	return NULL;
}


/*
@brief : burst file into segments and initiate burst workers
*/
void burst_file(const char * source_file_path, int segment_size)
{
	if (!validate_file(source_file_path)) {
		fprintf(stderr," %s not an valid file \n", source_file_path);
		exit(-1);
	}
	struct filename_attr name_attr;
	memset(&name_attr, 0, sizeof name_attr);
	strcpy(name_attr.source_path, source_file_path);
	if (b_output_dir) {
		char tmp[BUFSIZ] = {0};
		sprintf(tmp,"%s%c%s",output_directory,'/',base_file_name);
		split_file_by_extension(tmp, name_attr.suffix, name_attr.extension);
	}
	else {			
		split_file_by_extension(source_file_path, name_attr.suffix, name_attr.extension);
	}
	init_burst_attr_list();
	int fd = open(source_file_path, O_RDONLY);
	if (fd < 0) {
		perror("burst_file open : ");
		exit(-1);
	}
	pthread_t threads[workers_pool_size];
	int line = 0;
	int byte = 0;
	burst_attr_list[current_segment].startingByte = 0;
	char buf[READ_BLOCK_SIZE] = {0};	
	int n = 0;
	int i = 0;

	for (i= 0; i < workers_pool_size; ++i)
	{
		pthread_create(&threads[i], NULL, burst_worker , (void *)&name_attr);
	}
 
	while ((n = read(fd, buf, sizeof buf )) > 0)
	{
		for(i = 0; i < n; i++) {
			
			if (buf[i] == '\n') {
				line++;
				if (line % segment_size == 0) {
					if (current_segment + 1 >= burst_attr_list_size) {
						burst_attr_list = getBurstAttrList(burst_attr_list, burst_attr_list_size * 2);
						if (burst_attr_list) {
							burst_attr_list_size = burst_attr_list_size * 2;
						}
						//printf("reallocation : %d\n", burst_attr_list_size);

					}
					pthread_mutex_lock(&count_mutex);
					burst_attr_list[current_segment++].endingByte = byte;
					//signify the availability of new segment
					pthread_cond_signal(&count_threshold_cv);	
					pthread_mutex_unlock(&count_mutex);	
					burst_attr_list[current_segment].startingByte = byte + 1;
					line = 0;
				}
			}
			byte++;
		}
	}

	pthread_mutex_lock(&count_mutex);
	no_more_seg = 1; //signify that no more segments will be needed
	if (burst_attr_list[current_segment].startingByte == byte) {
		//do nothing
		//empty segment
	}
	else {	 
		burst_attr_list[current_segment++].endingByte = byte;
	}
	pthread_cond_broadcast(&count_threshold_cv);
	pthread_mutex_unlock(&count_mutex);
	//print_burst_attr_list(current_segment);	
	for (i= 0; i < workers_pool_size; ++i)
	{

		pthread_cond_broadcast(&count_threshold_cv);	
		pthread_join(threads[i], NULL);
	}
	printf("%d segment(s) created for %s file\n",current_segment, source_file_path);
	free(burst_attr_list);
}

void print_help(const char * program_name)
{
	printf("Usage: %s [OPTIONS]\n", program_name);
	printf("  -f file                   file\n");
	printf("  -s segment-length         size(in line(s) count) <default is 5 lines>\n");
	printf("  -p pool-size              thread pool size<default 10>\n");
	#ifdef URL_SUPPORT
	printf("  -u url                    url\n");
	#endif
	printf("  -o output-dir             output directory <default is same as of the source file> \n");
	printf("  -h, --help                print this help and exit\n");
	printf("\n");
}

int main(int argc, char * argv[])
{
	int             c;
	const char    * short_opt = "hf:s:d:p:o:u:";
	const char *program_name = argv[0];
	char file_path[BUFSIZ] = {0};
	char url[FILE_PATH_MAX] = {0};
	int segment_length = 500;	
	
	struct option   long_opt[] =
	{
		{"help",          no_argument,       NULL, 'h'},
		{"file",          required_argument, NULL, 'f'},
		{"size",          required_argument, NULL, 's'},
		{"poolsize",      required_argument, NULL, 'p'},
		{"output",        required_argument, NULL, 'o'},
		{"url",        	  required_argument, NULL, 'u'},
		{NULL,            0,                 NULL, 0  }
	};

	if (argc == 1)
	{
		fprintf(stderr,"%s : missing operands\n", program_name);
		print_help(program_name);
		return(-1);
	}

	while((c = getopt_long(argc, argv, short_opt, long_opt, NULL)) != -1)
	{
		switch(c)
		{
			case -1:       /* no more arguments */
			case 0:        /* long options toggles */
				break;

			case 'f':
				strncpy(file_path, optarg, sizeof file_path);
				break;

	#ifdef URL_SUPPORT	
			case 'u':
				strncpy(url, optarg, sizeof url);
				break;
	#endif
			case 's':
				segment_length = atoi(optarg);
				break;

			case 'o':
				strncpy(output_directory, optarg, sizeof output_directory);
				b_output_dir = 1;
				break;

			case 'p':
				workers_pool_size = atoi(optarg);
				break;

			case 'h':
				print_help(program_name);
				return(0);

			case ':':
			case '?':
				fprintf(stderr, "Try `%s --help' for more information.\n", program_name);
				return(-1);

			default:
				fprintf(stderr, "%s: invalid option -- %c\n", program_name, c);
				fprintf(stderr, "Try `%s --help' for more information.\n", program_name);
				return(-1);
		};
	};
	if (strlen(file_path) > 0 ) {	
		if (b_output_dir) {
			base_file_name = basename(file_path);
		}
		burst_file(file_path, segment_length);
	}
#ifdef	URL_SUPPORT
	else if (strlen(url) > 0) {
		base_file_name = basename(url);
		downloadURL(url, base_file_name);
		burst_file(base_file_name, segment_length);
	}
#endif
	return(0);
}
