#
Submitter

Job submission helper developed at UMD. Further developments and switch 
to github package at Drexel.

Authors: Steve Sclafani, Mike Richman, and Ryan Maunu

Extracted from
[here](https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/sandbox/csky_scripts/pks_1830_211/submitter.py).



# Usage Example
```
job_basedir = state.job_basedir 
job_dir = '{}/{}/ECAS_11yr/T_{:17.6f}'.format (
    job_basedir, ana_name,  T)
sub = Submitter (job_dir=job_dir, memory=8,  max_jobs=1000)
commands, labels = [], []
this_script = os.path.abspath (__file__)

fmt = '{} --mucut {} --angrescut {} --ccut {} --ethresh {} do-ps-sens  --n-trials {}' \
                        ' --gamma={:.3f} --dec_deg {} --model_name {}'  \
                        ' --seed={} --additionalpdfs={} --nn'
command = fmt.format ( this_script, mucut, angrescut, ccut, ethresh,  n_trials,
                          gamma, dec_deg, model_name, s, addpdf)
fmt = 'csky_sens_{:07d}_mc_{:03f}_cc_{:03f}_arc_{:03f}_ethresh_{:03f}_' \
            'gamma_{:.3f}_decdeg_{:04f}_seed_{:04d}_{}nn_{}'                               
label = fmt.format (
        n_trials, mucut, ccut, angrescut, 
        ethresh, gamma, dec_deg, s, addpdf, 
        model_name)
commands.append (command)
labels.append (label)

sub.dry = dry

if 'condor00' in hostname:
    sub.submit_condor00 (commands, labels)
else:
    sub.submit_npx4 (commands, labels)
```
