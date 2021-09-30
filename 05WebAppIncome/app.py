import flask
import pickle
import pandas as pd
import numpy as np

# Use pickle to load in the pre-trained model
with open(f'model/01bd_09production_model.pkl', 'rb') as f:
    model = pickle.load(f)

# Initialise the Flask app
app = flask.Flask(__name__, template_folder='templates',
                  static_url_path="/static")
# Set up the main route
@app.route('/', methods=['GET', 'POST'])
def main():
    if flask.request.method == 'GET':
        # Just render the initial form, to get input
        return(flask.render_template('index.html'))
    
    if flask.request.method == 'POST':
        # Extract the input
        edad = flask.request.form['edad']
        educacion = flask.request.form['educacion']
        empresa = flask.request.form['empresa']
        max_saldo = flask.request.form['max_saldo']
        decre_saldo = flask.request.form['decre_saldo']
        linea_tc = flask.request.form['linea_tc']
        deuda_tc = flask.request.form['deuda_tc']

        # Make DataFrame for model
        input_variables = pd.DataFrame([[edad, educacion, empresa,max_saldo,decre_saldo,linea_tc,deuda_tc]],
                                       columns=['EDAD', 'PADRON_INSTRUC', 'SUNAT_CONDICION','MAX_SALDO_ENTIDAD_X_12M_LOG','PROM_DEC_SALDO_TOTAL_12M_LOG','linea_tc','deuda_tc'],
                                       dtype=float,
                                       index=['input'])

        # Get the model's prediction
        input_variables.fillna(0, inplace=True)
        var_log=['MAX_SALDO_ENTIDAD_X_12M_LOG','PROM_DEC_SALDO_TOTAL_12M_LOG']
        for col in var_log:
            input_variables[col]=np.log10(input_variables[col]+1)
        input_variables['MTO_LINEA_TC_NO_UTIL_010'] = input_variables['linea_tc'] - input_variables['deuda_tc']
        input_variables.drop(columns=['linea_tc','deuda_tc'],inplace=True)
        prediction = model.predict(input_variables)[0]

        # Render the form again, but add in the prediction and remind user
        # of the values they input before
        return flask.render_template('index.html',
                                     original_input={'Edad':edad,
                                                     'Education':educacion,
                                                     'Empresa':empresa,
                                                     'max_saldo':max_saldo,
                                                     'decre_saldo':decre_saldo,
                                                     'linea_tc':linea_tc,
                                                     'deuda_tc':deuda_tc,},
                                     result="S/.{:,.0f}".format(prediction),
                                     )

if __name__ == '__main__':
    app.run(debug=True,port=5050)