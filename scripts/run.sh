#!/bin/bash

# Loading configurations for experiments
echo '>> Loading config file config.cfg'
source config.cfg

unset datasets
declare -A datasets
datasets[$db]=$db_defaults

unset test_lands
declare -A test_lands
test_lands[$db]=$db_lands

unset flags
declare -A flags
flags[$db]=$db_flags

echo -e '\n\n>> Creating directories ...'
mkdir -p $output_data

for dataset in ${!datasets[@]}
do
	dataset_path="$input_data"
	default=${datasets[${dataset}]}
	flag=${flags[${dataset}]}
	defaults=(`echo $default|tr "," "\n"`)
	experiments=(`echo $flag|tr "," "\n"`)

	echo ">> Processing dataset ${dataset} with default values (${defaults[@]})"
	echo ">> Experiment flags ${experiments[@]}"

	if [[ ${experiments[0]} -eq "1" ]]; then
		echo '---------------------------------------------------------'
		echo '      Compare Strategies for Connected Components 	   '
		echo '---------------------------------------------------------'

		OUTPUT="$output_data/ccs/"
		mkdir -p $OUTPUT

		echo "Running command ..."
		echo "$JVM $CC_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg lb=${defaults[3]} maxS=${defaults[4]}"
		echo "---- `date`"
		$JVM $CC_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg lb=${defaults[3]} maxS=${defaults[4]}
	fi

	if [[ ${experiments[1]} -eq "1" ]]; then
		echo '-----------------------------------------'
		echo '      Baselines Varying Landmarks        '
		echo '-----------------------------------------'

		OUTPUT="$output_data/baselines/"
		mkdir -p $OUTPUT

		lands=(`echo ${test_lands[${dataset}]}|tr "," "\n"`)

		for strategy in ${landmarkSelection[*]}
		do
			for l in ${lands[*]}
			do
				if [[ $landmarkAssignment == "ranking" ]]; then
					echo "Running command ..."
					echo "$JVM $BASELINE_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed store=${defaults[5]} isApproximate=false kind=edge"
					echo "---- `date`"
					$JVM $BASELINE_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=false kind=edge
				else
					for run in {1..10}
					do
						OUT2="$OUTPUT$run/"
						mkdir -p $OUT2
						echo "Running command ..."
                                        	echo "$JVM $BASELINE_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge"
                                        	echo "---- `date`"
                                 	       $JVM $BASELINE_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge
					done
				fi
			done
		done
	fi

	if [[ ${experiments[2]} -eq "1" ]]; then
		echo '-------------------------------------------------'
		echo '      Comparing Importance Factors       	       '
		echo '-------------------------------------------------'

		OUTPUT="$output_data/importance/"
		mkdir -p $OUTPUT

		lands=(`echo ${test_lands[${dataset}]}|tr "," "\n"`)

		for strategy in ${landmarkSelection[*]}
		do
			for l in ${lands[*]}
			do
				if [[ $landmarkAssignment == "ranking" ]]; then
					echo "Running command ..."
					echo "$JVM $ALPHAS_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=false kind=edge"
					echo "---- `date`"
					$JVM $ALPHAS_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=false kind=edge
				else
                                        for run in {1..10}
                                        do
                                                OUT2="$OUTPUT$run/"
						mkdir -p $OUT2
                                                echo "Running command ..."
                                                echo "$JVM $ALPHAS_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge"
                                                echo "---- `date`"
                                               $JVM $ALPHAS_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge
                                        done
                                fi
			done
		done
	fi

	if [[ ${experiments[3]} -eq "1" ]]; then
		echo '-------------------------------------------------'
		echo '      Comparing LS Strategies       	       '
		echo '-------------------------------------------------'

		OUTPUT="$output_data/selection/"
		mkdir -p $OUTPUT

		lands=(`echo ${test_lands[${dataset}]}|tr "," "\n"`)

		for l in ${lands[*]}
		do
			if [[ $landmarkAssignment == "ranking" ]]; then
				echo "Running command ..."
				echo "$JVM $LS_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=false kind=edge"
				echo "---- `date`"
				$JVM $LS_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=false kind=edge
			else
                        	for run in {1..10}
                                do
                                	OUT2="$OUTPUT$run/"
					mkdir -p $OUT2
                                        echo "Running command ..."
                                        echo "$JVM $LS_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge"
                                        echo "---- `date`"
                                       $JVM $LS_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=false kind=edge
                                done
                        fi
		done
	fi

	if [[ ${experiments[4]} -eq "1" ]]; then
		echo '---------------------------------------'
		echo '      Distances Varying Landmarks      '
		echo '---------------------------------------'

		OUTPUT="$output_data/queries"
		mkdir -p $OUTPUT
		if [[ $isApproximate == "true" ]]; then
			OUTPUT2="$OUTPUT/approx/"
		else
			OUTPUT2="$OUTPUT/real/"
		fi
		mkdir -p $OUTPUT2

		lands=(`echo ${test_lands[${dataset}]}|tr "," "\n"`)

		for strategy in ${landmarkSelection[*]}
		do
			for l in ${lands[*]}
			do
				if [[ $landmarkAssignment == "ranking" ]]; then
					echo "Running command ..."
					echo "$JVM $HYPERQ_jar dataFolder=${input_data} outputFolder=$OUTPUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=$isApproximate kind=$kind"
					echo "---- `date`"
					$JVM $HYPERQ_jar dataFolder=${input_data} outputFolder=$OUTPUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=$isApproximate kind=$kind
				else
                                        for run in {1..5}
                                        do
                                                OUT2="$OUTPUT2$run/"
						mkdir -p $OUT2
                                                echo "Running command ..."
                                                echo "$JVM $HYPERQ_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=$isApproximate kind=$kind"
                                                echo "---- `date`"
                                               $JVM $HYPERQ_jar dataFolder=${input_data} outputFolder=$OUT2 dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$run isApproximate=$isApproximate kind=$kind
                                        done
                                fi
			done
		done
	fi

        if [[ ${experiments[5]} -eq "1" ]]; then
                echo '-----------------------------'
                echo '      Search By Tag          '
                echo '-----------------------------'

                OUTPUT="$output_data/search/"
                mkdir -p $OUTPUT

                lands=(`echo ${test_lands[${dataset}]}|tr "," "\n"`)

                for strategy in ${landmarkSelection[*]}
                do
                        for l in ${lands[*]}
                        do
                        	echo "Running command ..."
                                echo "$JVM $SEARCH_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy numQueries=${defaults[2]} store=${defaults[5]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=$isApproximate kind=$kind"
                                echo "---- `date`"
                                $JVM $SEARCH_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg numLandmarks=$l samplePerc=${defaults[1]} landmarkSelection=$strategy store=${defaults[5]} numQueries=${defaults[2]} landmarkAssignment=$landmarkAssignment lb=${defaults[3]} maxS=${defaults[4]} alpha=$alpha beta=$beta seed=$seed isApproximate=$isApproximate kind=$kind
                        done
                done
        fi

        if [[ ${experiments[6]} -eq "1" ]]; then
                echo '---------------------------'
                echo '       Line Graph          '
                echo '---------------------------'

                OUTPUT="$output_data/lg/"
                mkdir -p $OUTPUT

                echo "Running command ..."
                echo "$JVM $LINE_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg maxS=${defaults[4]}"
                echo "---- `date`"
                $JVM $LINE_jar dataFolder=${input_data} outputFolder=$OUTPUT dataFile=${dataset}.hg maxS=${defaults[4]}
        fi
done
echo 'Terminated.'
