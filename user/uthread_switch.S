	.text

	/*
         * save the old thread's registers,
         * restore the new thread's registers.
         */

	.globl thread_switch
thread_switch:
	/* YOUR CODE HERE */
    sd ra, 0(a0)
	/* sd pc, 8(a0) */
    sd sp, 16(a0)
    sd gp, 24(a0)
    sd tp, 32(a0)
    sd s0, 40(a0)
    sd s1, 48(a0)
    sd s2, 56(a0)
    sd s3, 64(a0)
    sd s4, 72(a0)
    sd s5, 80(a0)
    sd s6, 88(a0)
    sd s7, 96(a0)
    sd s8, 104(a0)
    sd s9, 112(a0)
    sd s10, 120(a0)
    sd s11, 128(a0)
	   
	
    ld ra, 0(a1)
	/* ld pc, 8(a1) */
    ld sp, 16(a1)
    ld gp, 24(a1)
    ld tp, 32(a1)
    ld s0, 40(a1)
    ld s1, 48(a1)
    ld s2, 56(a1)
    ld s3, 64(a1)
    ld s4, 72(a1)
    ld s5, 80(a1)
    ld s6, 88(a1)
    ld s7, 96(a1)
    ld s8, 104(a1)
    ld s9, 112(a1)
    ld s10, 120(a1)
    ld s11, 128(a1)

	ret    /* return to ra */
