import {useEffect, useState} from "react";
import {Layout, Menu, Input, Spin} from "antd";
import './dashboard.scss';
import {AnimeCard} from "./AnimeCard";
import axios from 'axios'

const { Header, Content } = Layout;
const {Search} = Input;

const headers = {
    'Accept': '*/*',
    'Content-Type': 'application/json;charset=UTF-8',
    'Access-Control-Allow-Origin': '*',
    'crossDomain': true,
};

export const Dashboard = () => {
    const [populars, setPopulars] = useState([]);
    const [recommendations, setRecommendations] = useState([]);
    const [popularFetchTime, setPopularFetchTime] = useState("");
    const [recommendationsFetchTime, setRecommendationsFetchTime] = useState("");
    const [userSearch, setUserSearch] = useState("");
    const [isRecommendationsLoading, setRecommendationsLoading] = useState(false);

    useEffect(() => {
        const intervalId = setInterval(() => {
            getPopularRecommendations();
        }, 10000)
        console.log(intervalId)

    }, [])

    const getPopularRecommendations = () => {
        let startTime = Date.now();
        axios.get(" http://localhost:5001/top-recommendations", {headers} ).then(response => {
            setPopulars(response.data);

        }).catch(error => {
            console.log(error);
        }).finally(() => {
           setPopularFetchTime(parseFloat(Date.now() - startTime).toFixed(2).toString());
        });
    }

    const geUserRecommendations = (userName) => {
        let startTime = Date.now();
        setRecommendationsLoading(true);
        axios.get(" http://localhost:5001/user-recommendations?user=" + userName, {headers} ).then(response => {
            setRecommendations(response.data);
        }).catch(error => {
            console.log(error);
        }).finally(() => {
            setRecommendationsFetchTime(parseFloat(Date.now() - startTime).toFixed(2).toString());
            setRecommendationsLoading(false);
        });
    }

    const onSearchClicked = (text) => {
        if(text === '') {
            setUserSearch("");
            setRecommendations([]);
        }
        else {
            setUserSearch(text);
            geUserRecommendations(text);
        }
    };

    return (
        <div className="dashboard">
            <Layout className="anime-layout">
                <Header className="nav-bar">
                    <div className="logo" onClick={() => window.open("https://myanimelist.net/")}/>
                    <div className="title">Anime Recommendation System Using Spark</div>
                    <Menu
                        className="header-menu"
                        theme="dark"
                        mode="horizontal"
                        // items={[{key: 0, label:'Help'}]}
                    />
                </Header>
                <Content className="content-area">
                    <Search
                        className="user-search"
                        placeholder="Enter User ID"
                        enterButton="Recommend"
                        size="large"
                        loading={isRecommendationsLoading}
                        allowClear
                        onSearch={onSearchClicked} />
                    {recommendations && recommendations.length !== 0 && <>
                        <div className="popular-anime-title">Recommendations for {userSearch}</div>
                        <div className="fetch-time-display">{recommendationsFetchTime ? `Fetched in ${recommendationsFetchTime} ms` : "Fetching..."}</div>
                        <div className="recommendations-card-layout">
                            {recommendations.map((anime, key) => (
                                <AnimeCard
                                    key={key}
                                    imageUrl={anime.image_url}
                                    animeId={anime.anime_id}
                                    title={anime.title}
                                    related={anime.related}
                                    episodes={anime.episodes}
                                    genre={anime.genre}
                                />
                            ))}
                        </div>
                    </>
                    }
                    <div className="popular-anime-title">Trending</div>
                    <div className="fetch-time-display">{popularFetchTime ? `Fetched in ${popularFetchTime} ms` : "Fetching..."}</div>
                    <div className="recommendations-card-layout">
                        {populars && populars.length === 0 ? <Spin className="spin-loader" tip="Loading" size="large"/> : populars.map((anime, key) => (
                            <AnimeCard
                                key={key}
                                imageUrl={anime.image_url}
                                animeId={anime.anime_id}
                                title={anime.title}
                                related={anime.related}
                                episodes={anime.episodes}
                                genre={anime.genre}
                            />
                        ))}
                    </div>
                </Content>
            </Layout>

        </div>
    )
};