"""
Basecall rules.
"""

BASECALL_GUPPY_DIR_PATTERN = 'results/guppy/{sample}/{cell}'
BASECALL_GUPPY_FASTQ_PATTERN = '{cell}_guppy-{profile}_fastq_{fastq_set}.fastq.gz'
BASECALL_GUPPY_META_PATTERN = '{cell}_guppy_metadata.tar.gz'
BASECALL_GUPPY_INFO_PATTERN = '{cell}_info.tsv.gz'
BASECALL_GUPPY_MD5_PATTERN = '{cell}.md5'

#
# Functions
#

def get_fastq_iter(fastq_list):
    for in_file_name in fastq_list:
        with gzip.open(in_file_name, 'rt') as in_file:
            for record in Bio.SeqIO.parse(in_file, 'fastq'):
                yield record

#
# Rules
#

# ontbc_basecall_guppy
#
# Run Guppy basecaller.
rule ontbc_basecall_guppy:
    output:
        flag='results/guppy/{sample}/{cell}/basecall.flag'
    params:
        threads=6
    run:

        guppy_run_ok = True  # Fail rule if False, but do not remove data

        # Get cell entry
        cell_entry = ontbclib.rules.get_cell_entry(wildcards, CELL_TABLE, config)

        # Get run parameters
        cuda_device = config.get('cuda_device', '0')
        run_prefix = config.get('run_prefix', '')

        if run_prefix != '' and not run_prefix.endswith(' '):
            run_prefix += ' '

        # Get temp directory
        temp_dir = ontbclib.filesystem.get_temp_path(cell_entry, config, suffix='/basecall')

        temp_dir_guppy = os.path.join(temp_dir, 'guppy')

        print(f'Basecalling in: {temp_dir_guppy}')

        # Run attributes
        run_attrib_list = list()

        for cell_entry_key in ['sample', 'cell', 'fast5_dir', 'profile', 'guppy_dir', 'cfg']:
            run_attrib_list.append(pd.Series(
                ['RUNINFO', cell_entry_key, cell_entry[cell_entry_key]],
                index=['TYPE', 'ATTRIBUTE', 'VALUE']
            ))

        # Basecall and collect results
        try:
            os.makedirs(temp_dir_guppy)

            # Run Guppy
            guppy_dir = cell_entry['guppy_dir']
            fast5_dir = cell_entry['fast5_dir']
            cfg = cell_entry['cfg']

            run_attrib_list.append(pd.Series(
                ['TIMESTAMP', 'basecall_start', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                index=['TYPE', 'ATTRIBUTE', 'VALUE']
            ))

            shell(
                """{run_prefix}{guppy_dir}/bin/guppy_basecaller """
                    """-s {temp_dir_guppy} """
                    """-i {fast5_dir} """
                    """-c {cfg} """
                    """--compress_fastq """
                    """--num_alignment_threads {params.threads} """
                    """--device cuda:{cuda_device}"""
            )

            run_attrib_list.append(pd.Series(
                ['TIMESTAMP', 'basecall_end', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                index=['TYPE', 'ATTRIBUTE', 'VALUE']
            ))

            # Make output directory (in results)
            out_dir = BASECALL_GUPPY_DIR_PATTERN.format(**wildcards)

            os.makedirs(out_dir, exist_ok=True)

            # Get a list of output directories from guppy.
            dir_list = [val for val in os.listdir(temp_dir_guppy) if os.path.isdir(os.path.join(temp_dir_guppy, val))]

            # Check for pass and fail directories
            for req_dir in ['pass', 'fail']:
                if req_dir not in dir_list:
                    warn_msg = 'Missing expected FASTQ output directory from guppy run: {}'.format(
                        os.path.join(temp_dir_guppy, req_dir)
                    )

                    run_attrib_list.append(pd.Series(
                        ['WARNING', 'guppy_missing', warn_msg],
                        index=['TYPE', 'ATTRIBUTE', 'VALUE']
                    ))

                    raise RuntimeWarning(warn_msg)

                    guppy_run_ok = False

            # Collect FASTQ files
            for subdir in dir_list:
                print(f'Processing FASTQs in subdirectory: {subdir}')

                fastq_list = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.join(temp_dir_guppy, subdir)) for f in fn if f.endswith('.fastq.gz')]

                if len(fastq_list) == 0:
                    if subdir in {'pass', 'fail'}:
                        warn_msg = 'No FASTQ files in output subdirectory from guppy run: {}'.format(
                            os.path.join(temp_dir_guppy, subdir)
                        )

                        run_attrib_list.append(pd.Series(
                            ['WARNING', 'guppy_nofastq', warn_msg],
                            index=['TYPE', 'ATTRIBUTE', 'VALUE']
                        ))

                        raise RuntimeWarning(warn_msg)

                        guppy_run_ok = False

                    continue

                fastq_out = os.path.join(
                    out_dir,
                    BASECALL_GUPPY_FASTQ_PATTERN.format(cell=cell_entry['cell'], profile=cell_entry['profile'], fastq_set=subdir)
                )

                with Bio.bgzf.BgzfWriter(fastq_out, 'wt') as out_file:
                    Bio.SeqIO.write(get_fastq_iter(fastq_list), out_file, 'fastq')


            # Collect metadata files (non-FASTQ) from basecall process
            temp_meta = os.path.join(temp_dir, 'meta_file_list.txt')

            shell(
                """cd {temp_dir_guppy}; """
                """find . -not -name '*.fastq.gz' -and -not -type d """
                """> {temp_meta}"""
            )

            meta_out_file_name = os.path.abspath(
                os.path.join(out_dir, BASECALL_GUPPY_META_PATTERN.format(cell=cell_entry['cell']))
            )

            shell(
                """cd {temp_dir_guppy}; """
                """tar -zcf {meta_out_file_name} -T {temp_meta}"""
            )

            # Write info TSV
            df_run = pd.concat(run_attrib_list, axis=1).T

            run_info_out = os.path.join(out_dir, BASECALL_GUPPY_INFO_PATTERN.format(cell=cell_entry['cell']))

            df_run.to_csv(run_info_out, sep='\t', index=False, compression='gzip')

            # MD5
            md5_out_file_name = os.path.join(BASECALL_GUPPY_MD5_PATTERN.format(cell=cell_entry['cell']))

            shell(
                """cd {out_dir}; """
                """md5sum $(find . -type f | grep -Fv {md5_out_file_name}) > {md5_out_file_name}"""
            )


        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        # Write flag
        with open(output.flag, 'wt') as out_file:
            out_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S\n'))
