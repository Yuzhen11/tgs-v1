#ifndef SHMTOOL_H
#define SHMTOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#define SHM_DIR "/tmp"

struct shared_mem
{
	char service_num;
	key_t key;
	int shmid;
	void *segptr;
	bool is_creator;//creator loads the index to shm, others are allowed to read only

	shared_mem(size_t seg_size)
	{
		service_num = 'a';
		key = ftok(SHM_DIR, service_num);
		if((shmid = shmget(key, seg_size, IPC_CREAT|IPC_EXCL|0666)) == -1)
		{
			is_creator=false;//shm already created by others, go to read-only mode
			if((shmid = shmget(key, seg_size, 0)) == -1)
			{
				perror("shmget() error in shared_mem's constructor");
				exit(1);
			}
			//---------------------------------------------
			if((segptr = shmat(shmid, 0, SHM_RND|SHM_RDONLY)) == (char*) -1)
			{
				perror("shmat() error in shared_mem's constructor");
				exit(1);
			}
		}
		else
		{
			is_creator=true;
			//---------------------------------------------
			if((segptr = shmat(shmid, 0, SHM_RND)) == (char*) -1)
			{
				perror("shmat() error in shared_mem's constructor");
				exit(1);
			}
		}
	}

	bool creator()
	{
		return is_creator;
	}

	void* get_head()
	{
		return segptr;
	}

	~shared_mem()
	{
		shmctl(shmid, IPC_RMID, 0);
	}
};

#endif
