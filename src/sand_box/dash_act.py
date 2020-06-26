#!/usr/bin/env python
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from src.sand_box.backend import dt_year, dt_dptos, dt_mpios, get_mpio_by_dpto, dt_at, get_act_data, get_table_fig
from dash.dependencies import Output, Input, State
from dash.exceptions import PreventUpdate

PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"

NAVBAR = dbc.Navbar(
    children=[
        html.A(
            # Use row and col to control vertical alignment of logo / brand
            dbc.Row(
                [
                    dbc.Col(html.Img(src=PLOTLY_LOGO, height="30px")),
                    dbc.Col(
                        dbc.NavbarBrand("Datos de actividad FE - Dashboard", className="ml-2")
                    ),
                ],
                align="center",
                no_gutters=True,
            ),
            href="https://plot.ly",
        )
    ],
    color="dark",
    dark=True,
    sticky="top",
)

LEFT_COLUMN = dbc.Jumbotron(
    [
        html.H4(children="Opciones de filtro", className="display-5"),
        html.Hr(className="my-2"),

        html.Label("Selección temporal", className="lead"),
        dcc.Dropdown(
            id="drop-years",
            clearable=True,
            multi=True,
            style={"marginBottom": 10, "font-size": 12},
            options=dt_year,
            placeholder="Año",
            # value=[dt_year[0]['value']],
            searchable=True,
        ),

        html.Label("Seleccione espacial", style={"marginTop": 20}, className="lead"),
        dcc.Dropdown(
            id="drop-departamentos",
            clearable=True,
            multi=True,
            style={"marginBottom": 10, "font-size": 12},
            options=dt_dptos,
            placeholder="Departamento",
            # value=[dt_dptos[0]['value']],
            searchable=True
        ),
        dbc.Button(
            id="submit-dpto",
            n_clicks=0,
            children='Enviar',
            size="sm",
        ),
        html.Br(),
        html.Br(),

        dcc.Dropdown(
            id="drop-municipios",
            clearable=True,
            multi=True,
            style={"marginBottom": 10, "font-size": 12, 'max-height': '150px',
                   'overflow-y': 'Auto'},
            options=dt_mpios,
            placeholder="Municipios",
            searchable=True
        ),
        html.Label("Seleccione por categoría IPCC", style={"marginTop": 20}, className="lead"),
        dcc.Dropdown(
            id="drop-ipcc",
            clearable=True,
            multi=True,
            style={"marginBottom": 10, "font-size": 10},
            options=dt_at,
            placeholder="IPCC",
            # value=['2'],
        ),
        dbc.Button(
            id="submit-run",
            n_clicks=0,
            children='Consultar',
            outline=True
        ),
    ]
)

MIDDLE_COLUMN = [
    dbc.CardHeader(html.H5("Resultados de la consulta")),
    dbc.CardBody(
        [
            dcc.Loading(
                id="table-res",
                type='circle',
                # type="default",
                children=[
                    dbc.Alert(
                        "Not enough data to render this plot, please adjust the filters",
                        id="no-data-alert-bank",
                        color="warning",
                        style={"display": "none"},
                    ),
                    dcc.Graph(id='plot-map', style=dict(aling='centered')),
                    dcc.Graph(id="plot-table"),
                ],
            )
        ],
        style={"marginTop": 0, "marginBottom": 0, 'display': 'flex'},
    ),
]

BODY = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(LEFT_COLUMN, md=4),
                dbc.Col(dbc.Card(MIDDLE_COLUMN), md=8),
                # dbc.Col(dbc.Card(RIGHT_COLUMN), md=3),
            ],
            style={"marginTop": 30},
        ),
    ],
    className="mt-12",
)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = html.Div(children=[NAVBAR, BODY])


@app.callback(
    Output("drop-municipios", "value"),
    [
        Input("submit-dpto", "n_clicks")
    ],
    [
        State("drop-departamentos", "value")
    ]
)
def update_mpios(n_clicks, dpto):
    if dpto:
        return get_mpio_by_dpto(dpto)
    else:
        return []


def wait_for():
    return dbc.CardBody(
        [
            dcc.Loading(
                id="table-res",
                children=[
                    dbc.Alert(
                        "Not enough data to render this plot, please adjust the filters",
                        id="no-data-alert-bank",
                        color="warning",
                        style={"display": "none"},
                    ),
                ],
                type="default",
            )
        ],
        style={"marginTop": 0, "marginBottom": 0, 'display': 'flex'},
    )


@app.callback(
    [
        Output('plot-table', 'figure'),
        Output('plot-map', 'figure')
    ],
    [
        Input("submit-run", 'n_clicks')
    ],
    [
        State("drop-years", 'value'),
        State("drop-departamentos", 'value'),
        State("drop-municipios", 'value'),
        State("drop-ipcc", 'value'),
    ],
)
def update_query(n_clicks, year, dpto, mpio, ipcc):
    if ((year is None) or (len(year) == 0)) or ((mpio is None) or (len(mpio) == 0)) or \
            ((ipcc is None) or (len(ipcc) == 0)):
        raise PreventUpdate
    else:
        df = get_act_data(year, dpto, mpio, ipcc)
        return get_table_fig(df, mpio)


if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port='8051', debug=True)
    pass
