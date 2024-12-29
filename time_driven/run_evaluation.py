import configparser
import subprocess

def run_program(executable_name, mode, input_file, d, s, step, end):
    while d <= end:
        slide = d / s
        slide_name = int(slide)
        output_file_name = f"sliding_{slide_name}.csv"
        run_command = f'{executable_name} {input_file} {mode} {d} {s} > /home/maurofama/Documents/PolyFlow/tesi/c-frames/evaluation/results/td/sb/eager_sort/{output_file_name}'
        try:
            subprocess.check_call(run_command, shell=True)
            print(f"Esecuzione completata con d={d}, s={s}. Output salvato in {output_file_name}")
        except subprocess.CalledProcessError as e:
            print(f"Errore nell'esecuzione con d={d}, s={s}: {e}")
        d += step

def main(config_file_path, c_file_path):
    config = configparser.ConfigParser()
    config.read(config_file_path)

    executable_name = './tw_sb_executable'
    compile_command = f'gcc {c_file_path} -o {executable_name}'

    # Compila il programma C
    try:
        subprocess.check_call(compile_command, shell=True)
        print("Compilazione completata con successo.")
    except subprocess.CalledProcessError:
        print("Errore nella compilazione.")
        return

    # Esegue il programma per ogni sezione in config.ini
    for section in config.sections():
        mode = config[section]['mode']
        input_file = config[section]['input_file']
        d = int(config[section]['d'])
        s = int(config[section]['s'])
        step = int(config[section]['step'])
        end = int(config[section]['end'])

        print(f"\nEsecuzione per la sezione: {section}")
        run_program(executable_name, mode, input_file, d, s, step, end)

if __name__ == "__main__":
    config_file_path = 'config.ini'
    c_file_path = 'tw_sb.c'
    main(config_file_path, c_file_path)
